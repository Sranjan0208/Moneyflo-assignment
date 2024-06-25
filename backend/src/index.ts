import express, { Request, Response } from "express";
import { Pool } from "pg";
import fs from "fs";
import csv from "csv-parser";
import path from "path";
import { parseISO, isValid } from "date-fns";
import cors from "cors";

require("dotenv").config();

const app = express();
const port = 3000;

app.use(cors());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const createTable = async (): Promise<void> => {
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
    await pool.query(createTableQuery);
    console.log("Table created or already exists.");
  } catch (err) {
    console.error("Error creating table:", err);
  }
};

interface CSVRow {
  order_item_id: string;
  order_id: string;
  order_date: string;
  order_approval_date: string;
  order_item_status: string;
  sku: string;
  fsn: string;
  product_title: string;
  quantity: string;
  order_cancellation_date: string;
  procurement_sla: string;
  procurement_after_date: string;
  procurement_by_date: string;
  procurement_sla_breached: string;
  dispatch_after_sla: string;
  procurement_ready_after_date: string;
  procurement_dispatch_sla: string;
  dispatch_after_date: string;
  dispatch_by_date: string;
  order_ready_for_dispatch_on_date: string;
  dispatched_date: string;
  dispatch_sla_breached: string;
  seller_pickup_reattempts: string;
  delivery_sla: string;
  deliver_by_date: string;
  order_delivery_date: string;
}

const parseCSVValue = (value: string, isTimestamp: boolean = false) => {
  if (!value || value.trim() === "") {
    return null;
  }
  if (isTimestamp) {
    const parsedDate = parseISO(value);
    return isValid(parsedDate) ? parsedDate.toISOString() : null;
  }
  return value;
};

const importCSVData = async (csvFilePath: string): Promise<void> => {
  console.log("started");

  const results: CSVRow[] = [];
  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on("data", (data: CSVRow) => results.push(data))
    .on("end", async () => {
      console.log("started2");

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
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
          await client.query(insertQuery, values);
        }
        await client.query("COMMIT");
        console.log("Data imported successfully.");
      } catch (err) {
        await client.query("ROLLBACK");
        console.error("Error importing data:", err);
      } finally {
        client.release();
      }
    });
};

app.get("/order-status", async (req: Request, res: Response) => {
  try {
    const result = await pool.query(`
            SELECT order_item_status, COUNT(*) AS count
            FROM sales_data
            GROUP BY order_item_status
        `);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send("Server Error");
  }
});

app.listen(port, async () => {
  await createTable();
  const csvFilePath = path.resolve(__dirname, "../assets/data.csv");
  await importCSVData(csvFilePath);
  console.log(`Server running on port ${port}`);
});
