const puppeteer = require('puppeteer-core');
const chromium = require('@sparticuz/chromium');
const path = require('path');
const fs = require('fs');
const parquet = require('parquetjs-lite');
const csv = require('csv-parser');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

exports.handler = async (event) => {
  try {
    console.log('Iniciando o navegador...');
    const browser = await puppeteer.launch({
      args: chromium.args,
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
      defaultViewport: { width: 1280, height: 720 },
    });

    const page = await browser.newPage();

    const downloadPath = '/tmp/downloads';
    fs.mkdirSync(downloadPath, { recursive: true });

    console.log('Configurando o diretório de downloads...');
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: downloadPath,
    });

    console.log('Navegando para a página...');
    await page.goto('https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br');

    console.log('Clicando no botão de download...');
    await page.click('a[href="/indexPage/day/IBOV?language=pt-br"]');

    console.log('Aguardando o download ser concluído...');
    await page.waitForTimeout(2000);

    console.log('Fechando o navegador...');
    await browser.close();

    const files = fs.readdirSync(downloadPath);
    const csvFiles = files.filter(file => file.endsWith('.csv'));
    if (csvFiles.length === 0) {
      throw new Error('Nenhum arquivo CSV encontrado na pasta de downloads.');
    }
    const csvFilePath = path.resolve(downloadPath, csvFiles[0]);

    // Extraindo a data do nome do arquivo
    const fileName = path.basename(csvFilePath, '.csv'); // Exemplo: "IBOVDia_19-11-24"
    const dateMatch = fileName.match(/(\d{2})-(\d{2})-(\d{2})/);
    if (!dateMatch) {
      throw new Error('Data não encontrada no nome do arquivo.');
    }

    // Reformatar a data para ISO (YYYY-MM-DD)
    const [day, month, year] = dateMatch.slice(1); // Captura "19", "11", "24"
    const formattedYear = `20${year}`; // Adicionar "20" ao ano
    const dateStr = `${formattedYear}-${month}-${day}`; // Formato final: "2024-11-19";

    // Caminho no S3 com partição diária
    const s3Path = `Raw/date=${dateStr}/IBOVDia.parquet`;

    const parquetFilePath = path.resolve(downloadPath, 'IBOVDia.parquet');

    console.log(`Convertendo o arquivo ${csvFilePath} para Parquet...`);

    // Remover a primeira linha do arquivo CSV
    const tempFilePath = path.resolve(downloadPath, 'temp.csv');
    const lines = fs.readFileSync(csvFilePath, { encoding: 'latin1' }).split('\n');
    fs.writeFileSync(tempFilePath, lines.slice(1).join('\n'), { encoding: 'latin1' });

    const schema = new parquet.ParquetSchema({
      codigo: { type: 'UTF8' },
      acao: { type: 'UTF8' },
      tipo: { type: 'UTF8' },
      qtde_teorica: { type: 'UTF8' },
      part: { type: 'DOUBLE' },
    });

    const writer = await parquet.ParquetWriter.openFile(schema, parquetFilePath);

    let rowCount = 0;
    const rows = []; // Armazena todas as linhas temporariamente

    // Leitura do CSV
    await new Promise((resolve, reject) => {
      fs.createReadStream(tempFilePath, { encoding: 'latin1' })
        .pipe(
          csv({
            separator: ';',
            mapHeaders: ({ header }) => {
              const headerMap = {
                'Código': 'codigo',
                'Ação': 'acao',
                'Tipo': 'tipo',
                'Qtde. Teórica': 'qtde_teorica',
                'Part. (%)': 'part',
              };
              const mappedHeader = headerMap[header] || header;
              console.log(`Mapeando cabeçalho: ${header} -> ${mappedHeader}`);
              return mappedHeader;
            },
          })
        )
        .on('data', (row) => {
          rows.push(row);
        })
        .on('end', async () => {
          // Remover as duas últimas linhas
          rows.splice(-2);
          // Gravação no Parquet
          for (const row of rows) {
            const mappedRow = {
              codigo: row.codigo,
              acao: row.acao,
              tipo: row.tipo,
              qtde_teorica: row.qtde_teorica,
              part: parseFloat(row.part.replace(',', '.')),
            };

            if (mappedRow.codigo && mappedRow.acao && mappedRow.tipo && mappedRow.qtde_teorica && !isNaN(mappedRow.part)) {
              await writer.appendRow(mappedRow);
              rowCount++;
            } else {
              console.log(`Linha ignorada: ${JSON.stringify(row)}`);
            }
          }

          await writer.close();
          console.log(`Arquivo convertido para Parquet com sucesso. Total de linhas: ${rowCount}`);

          // Enviar o arquivo Parquet para o bucket S3
          const s3Client = new S3Client({ region: 'us-east-1' });
          const bucketName = 'data-lake-tc2-data';
          const keyName = s3Path;

          const fileContent = fs.readFileSync(parquetFilePath);

          const params = {
            Bucket: bucketName,
            Key: keyName,
            Body: fileContent,
          };

          const command = new PutObjectCommand(params);
          await s3Client.send(command);
          console.log('Arquivo enviado com sucesso para o S3!');
          resolve();
        })
        .on('error', (error) => {
          console.error('Erro ao processar o arquivo CSV:', error);
          reject(error);
        });
    });
  } catch (error) {
    console.error('Erro:', error);
  }
};