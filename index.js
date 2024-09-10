require("dotenv").config();
const fastify = require("fastify")({ logger: true });
const mysql = require("mysql2/promise");

// Database connection
let connection;

async function connectDB() {
  connection = await mysql.createConnection({
    host: "localhost",
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
  });
  console.log("Connected to MySQL");
}

function randomDateOfBirth() {
  const start = new Date(1950, 0, 1);
  const end = new Date(2020, 11, 31);
  return new Date(
    start.getTime() + Math.random() * (end.getTime() - start.getTime())
  );
}

fastify.get("/populate", async (_, reply) => {
  const insertQuery = "INSERT INTO users (name, date_of_birth, email) VALUES ?";
  const batchSize = 10000;

  for (let batch = 0; batch < 4000; batch++) {
    const users = [];
    for (let i = 0; i < batchSize; i++) {
      users.push([
        `User${batch * batchSize + i}`,
        randomDateOfBirth().toISOString().split("T")[0],
        `user${batch * batchSize + i}@example.com`,
      ]);
    }

    await connection.query(insertQuery, [users]);
  }
  reply.send({ message: "40M users inserted with random dates of birth" });
});

fastify.get("/select/without-index", async (_, reply) => {
  const start = Date.now();
  const [rows] = await connection.query(
    "SELECT * FROM users WHERE date_of_birth BETWEEN '1950-01-01' AND '2000-12-31'"
  );
  const duration = Date.now() - start;
  reply.send({ duration, rows_count: rows.length });
});

fastify.get("/select/btree-index", async (_, reply) => {
  // Check if the index already exists
  const [existingIndex] = await connection.query(`
      SELECT COUNT(1) AS indexExists 
      FROM INFORMATION_SCHEMA.STATISTICS 
      WHERE table_schema = '${process.env.MYSQL_DATABASE}' 
      AND table_name = 'users' 
      AND index_name = 'idx_dob_btree'
  `);

  if (existingIndex[0].indexExists === 0) {
    // If the index does not exist, create it
    await connection.query(
      "CREATE INDEX idx_dob_btree ON users (date_of_birth) USING BTREE"
    );
  }

  // Execute the select query
  const start = Date.now();
  const [rows] = await connection.query(
    "SELECT * FROM users WHERE date_of_birth BETWEEN '1950-01-01' AND '2000-12-31'"
  );
  const duration = Date.now() - start;

  reply.send({ duration, rows_count: rows.length });
});

fastify.get("/insert", async (request, reply) => {
  const insertQuery =
    "INSERT INTO users (name, date_of_birth, email) VALUES (?, ?, ?)";

  const start = Date.now();
  for (let i = 0; i < 10000; i++) {
    await connection.query(insertQuery, [
      `User${i}`,
      randomDateOfBirth().toISOString().split("T")[0],
      `user${i}@example.com`,
    ]);
  }
  const duration = Date.now() - start;
  reply.send({ duration });
});

const start = async () => {
  try {
    await connectDB();
    await fastify.listen({ port: 3000 });
    console.log("Server running on http://localhost:3000");
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
