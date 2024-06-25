"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const pg_1 = require("pg");
const fs_1 = __importDefault(require("fs"));
const csv_parser_1 = __importDefault(require("csv-parser"));
const path_1 = __importDefault(require("path"));
const date_fns_1 = require("date-fns");
const cors_1 = __importDefault(require("cors"));
require("dotenv").config();
const app = (0, express_1.default)();
const port = 3000;
app.use((0, cors_1.default)());
const pool = new pg_1.Pool({
    connectionString: process.env.DATABASE_URL,
});
const createTable = () => __awaiter(void 0, void 0, void 0, function* () {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS sales_data (
            order_item_id VARCHAR PRIMARY KEY,
            order_id VARCHAR,
            order_date DATE,
            order_approval_date TIMESTAMP,
            order_item_status VARCHAR,
            sku VARCHAR,
            fsn VARCHAR,
            product_title TEXT,
            quantity INTEGER,
            order_cancellation_date TIMESTAMP,
            procurement_sla INTEGER,
            procurement_after_date TIMESTAMP,
            procurement_by_date TIMESTAMP,
            procurement_sla_breached CHAR(1),
            dispatch_after_sla INTEGER,
            procurement_ready_after_date TIMESTAMP,
            procurement_dispatch_sla INTEGER,
            dispatch_after_date TIMESTAMP,
            dispatch_by_date TIMESTAMP,
            order_ready_for_dispatch_on_date TIMESTAMP,
            dispatched_date DATE,
            dispatch_sla_breached CHAR(1),
            seller_pickup_reattempts CHAR(1),
            delivery_sla INTEGER,
            deliver_by_date TIMESTAMP,
            order_delivery_date TIMESTAMP
        );
    `;
    try {
        yield pool.query(createTableQuery);
        console.log("Table created or already exists.");
    }
    catch (err) {
        console.error("Error creating table:", err);
    }
});
const parseCSVValue = (value, isTimestamp = false) => {
    if (!value || value.trim() === "") {
        return null;
    }
    if (isTimestamp) {
        const parsedDate = (0, date_fns_1.parseISO)(value);
        return (0, date_fns_1.isValid)(parsedDate) ? parsedDate.toISOString() : null;
    }
    return value;
};
const importCSVData = (csvFilePath) => __awaiter(void 0, void 0, void 0, function* () {
    console.log("started");
    const results = [];
    fs_1.default.createReadStream(csvFilePath)
        .pipe((0, csv_parser_1.default)())
        .on("data", (data) => results.push(data))
        .on("end", () => __awaiter(void 0, void 0, void 0, function* () {
        console.log("started2");
        const client = yield pool.connect();
        try {
            yield client.query("BEGIN");
            for (const row of results) {
                console.log("started3");
                const insertQuery = `
                        INSERT INTO sales_data (
                            order_item_id, order_id, order_date, order_approval_date, order_item_status, sku, fsn, product_title,
                            quantity, order_cancellation_date, procurement_sla, procurement_after_date, procurement_by_date,
                            procurement_sla_breached, dispatch_after_sla, procurement_ready_after_date, procurement_dispatch_sla,
                            dispatch_after_date, dispatch_by_date, order_ready_for_dispatch_on_date, dispatched_date,
                            dispatch_sla_breached, seller_pickup_reattempts, delivery_sla, deliver_by_date, order_delivery_date
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
                        ) ON CONFLICT (order_item_id) DO NOTHING;
                    `;
                const values = [
                    row.order_item_id,
                    row.order_id,
                    parseCSVValue(row.order_date, true),
                    parseCSVValue(row.order_approval_date, true),
                    row.order_item_status,
                    row.sku,
                    row.fsn,
                    row.product_title,
                    parseCSVValue(row.quantity),
                    parseCSVValue(row.order_cancellation_date, true),
                    parseCSVValue(row.procurement_sla),
                    parseCSVValue(row.procurement_after_date, true),
                    parseCSVValue(row.procurement_by_date, true),
                    row.procurement_sla_breached,
                    parseCSVValue(row.dispatch_after_sla),
                    parseCSVValue(row.procurement_ready_after_date, true),
                    parseCSVValue(row.procurement_dispatch_sla),
                    parseCSVValue(row.dispatch_after_date, true),
                    parseCSVValue(row.dispatch_by_date, true),
                    parseCSVValue(row.order_ready_for_dispatch_on_date, true),
                    parseCSVValue(row.dispatched_date, true),
                    row.dispatch_sla_breached,
                    row.seller_pickup_reattempts,
                    parseCSVValue(row.delivery_sla),
                    parseCSVValue(row.deliver_by_date, true),
                    parseCSVValue(row.order_delivery_date, true),
                ];
                yield client.query(insertQuery, values);
            }
            yield client.query("COMMIT");
            console.log("Data imported successfully.");
        }
        catch (err) {
            yield client.query("ROLLBACK");
            console.error("Error importing data:", err);
        }
        finally {
            client.release();
        }
    }));
});
app.get("/order-status", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const result = yield pool.query(`
            SELECT order_item_status, COUNT(*) AS count
            FROM sales_data
            GROUP BY order_item_status
        `);
        res.json(result.rows);
    }
    catch (err) {
        console.error(err);
        res.status(500).send("Server Error");
    }
}));
app.listen(port, () => __awaiter(void 0, void 0, void 0, function* () {
    yield createTable();
    const csvFilePath = path_1.default.resolve(__dirname, "../assets/data.csv");
    yield importCSVData(csvFilePath);
    console.log(`Server running on port ${port}`);
}));
