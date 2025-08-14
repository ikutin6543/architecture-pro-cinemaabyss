const express = require('express');

const http = require('http');
const https = require('https');

const app = express();

const MONOLITH_URL = process.env["MONOLITH_URL"]
const MOVIES_SERVICE_URL = process.env["MOVIES_SERVICE_URL"]
const MOVIES_MIGRATION_PERCENT = parseInt(process.env["MOVIES_MIGRATION_PERCENT"], 10)
const PORT = process.env["PORT"]

const api = express.Router();

const proxy = (req, res, {target}) => {
    const {headers, method, originalUrl} = req
    fetch(`${target}${originalUrl}`, {
        method,
        headers,
    }).then(async (response) => {
        const json = await response.json()
        res.status(response.status).send(json)
    }).catch((e) => {
        console.error(e)
        res.status(500).send()
    })
}


api.get('/users', (req, res) => {
    proxy(req, res, {target: MONOLITH_URL});
})

api.get('/movies', (req, res) => {
    const random = Math.random();

    if (random < MOVIES_MIGRATION_PERCENT / 100) {
        console.log(`to ${MOVIES_SERVICE_URL}`)
        proxy(req, res, {target: MOVIES_SERVICE_URL});
    } else {
        console.log(`to ${MONOLITH_URL}`)
        proxy(req, res, {target: MONOLITH_URL});
    }
})

app.get("/health", (req, res) => {
    res.status(200).send();
})

app.use("/api", api);

app.listen(PORT, () => {
    console.log(`proxy running on port ${PORT}`);
});