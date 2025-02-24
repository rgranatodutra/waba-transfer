import axios from "axios";
import { createPool } from "mysql2/promise";
import fs from "fs";
import path from "path";

async function processFile(row, index, total) {
    try {
        console.log(`${new Date().toLocaleString()} - Download ${index}/${total}`);

        const fileName = row["NOME_ARQUIVO"];
        const localFilePath = path.resolve("C:\\files", fileName);

        if (fs.existsSync(localFilePath)) {
            console.log(`${new Date().toLocaleString()} - Arquivo ${fileName} já existe, pulando download.`);
            return;
        }

        const reqUrl = "http://172.22.197.92:7001/" + fileName;
        const response = await axios.get(reqUrl, { responseType: "stream", timeout: 10000 });

        const message = await new Promise((res, rej) => {
            const writer = fs.createWriteStream(localFilePath);

            response.data.pipe(writer);

            writer.on("finish", () => {
                res(`Arquivo ${fileName} salvo com sucesso!`);
            });

            writer.on("error", (err) => {
                rej(new Error(`Falha ao salvar arquivo ${fileName}: ${err.message}`));
            });

            writer.on("close", () => {
                rej(new Error(`Conexão fechada antes de salvar o arquivo ${fileName}`));
            });

            setTimeout(() => {
                rej(new Error(`Timeout ao salvar arquivo ${fileName}`));
            }, 5000);
        });

        console.log(`${new Date().toLocaleString()} - ${message}`);
    } catch (err) {
        console.log(`${new Date().toLocaleString()} - Erro ao processar arquivo: ${err.message}`);
    }
}

async function app() {
    const pool = createPool({
        host: "127.0.0.1",
        user: "root",
        password: "1nf0tec",
        database: "crm_sgr",
    });

    try {
        const messagesQuery = "SELECT wf.NOME_ARQUIVO FROM w_mensagens wm"
            + " RIGHT JOIN w_mensagens_arquivos wf ON wf.CODIGO_MENSAGEM = wm.CODIGO"
            + " WHERE wm.DATA_HORA >= '2024-08-01'"
            + " ORDER BY wm.DATA_HORA DESC";

        const [rows] = await pool.query(messagesQuery);

        const total = rows.length;
        const batchSize = 10;

        for (let i = 0; i < total; i += batchSize) {
            const batch = rows.slice(i, i + batchSize).map((row, index) => processFile(row, i + index + 1, total));
            await Promise.all(batch);
        }
    } catch (err) {
        console.log(`${new Date().toLocaleString()} - Erro ao executar a aplicação: ${err.message}`);
    } finally {
        await pool.end();
        console.log(`${new Date().toLocaleString()} - Conexão com o banco de dados encerrada.`);
    }
}

app();