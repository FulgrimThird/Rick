"use strict";
const fs = require("fs");
const { Pool } = require("pg");
const axios = require("axios");

// Настройки Яндекса
const prodConfig = {
  connectionString:
    "postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync("CA.pem").toString(),
  },
};

// Настройки для тестовой базы данных
const debugConfig = {
  user: "Rick",
  host: "194.120.116.148",
  database: "Rick",
  password: "qweasdzxc",
};
// Имя Таблици
const tableName = "FulgrimThird"; 

const createTableQuery = `
  CREATE TABLE IF NOT EXISTS ${tableName} (
    id SERIAL PRIMARY KEY,
    name TEXT,
    data JSONB
  );
`;
//Вставляем данные в таблицу
const insertCharacters = async (characters, pool, logShow) => {
  const client = await pool.connect();
  const insertQuery = `INSERT INTO ${tableName} (name, data) VALUES ($1, $2)`;
  const values = characters.map(character => [character.name, character]);

  try {
    await client.query('BEGIN');
    const promises = values.map(([name, data]) =>
      client.query(insertQuery, [name, data])
    );
    await Promise.all(promises);
    await client.query('COMMIT');

    if (logShow) {
      values.forEach(([name]) => console.log(`Персонаж добавлен в базу данных: ${name}`));
    }
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Ошибка при вставке данных:', err.message);
  } finally {
    client.release();
  }
};
// Получаем страницы
const fetchPage = async (page, pool) => {
  try {
    const response = await axios.get(
      `https://rickandmortyapi.com/api/character/?page=${page}`,
    );
    const characters = response.data.results;
    await insertCharacters(characters, pool,false);
  } catch (error) {
    console.error(`Ошибка при загрузке страницы ${page}:`, error.message);
  }
};
//Bltv gj gthcjyf;fv
const fetchCharacters = async (pool, pagesAtOnce) => {
  try {
    const initialResponse = await axios.get(
      `https://rickandmortyapi.com/api/character/`,
    );
    const totalPages = initialResponse.data.info.pages;

    let page = 1;
    const pageGroups = [];

    while (page <= totalPages) {
      const tasks = [];
      for (let i = 0; i < pagesAtOnce && page <= totalPages; i++, page++) {
        tasks.push(fetchPage(page, pool));
      }
      pageGroups.push(Promise.all(tasks));
    }

    const startTime = Date.now();
    await Promise.all(pageGroups);
    const endTime = Date.now();

    console.log(`Данные успешно загружены`);
    console.log(
      `Время: ${(endTime - startTime) / 1000} секунд`,
    );
  } catch (error) {
    console.error("Ошибка при загрузке данных:", error.message);
  } finally {
    pool.end((err) => {
      if (err) {
        console.error(
          "Ошибка при закрытия соединения с базой:",
          err.message,
        );
      } else {
        console.log("Соединение с базой закрыто.");
      }
    });
  }
};
//Main и в Африке Main mode дебаг-моя база, релиз - боевая
const main = async (maxConnections, pagesAtOnce, mode) => {
  const config =
    mode === "release"
      ? { ...prodConfig, max: maxConnections }
      : { ...debugConfig, max: maxConnections };
  const pool = new Pool(config);
  const client = await pool.connect();
  try {
    await client.query(createTableQuery);
    console.log("Таблица успешно создана или уже существует.");
    await fetchCharacters(pool, pagesAtOnce);
  } catch (err) {
    console.error("Ошибка при создании таблицы:", err.message);
  } finally {
    client.release();
  }
};

//  Поехали!!!! Подбирается имперически
main(60,20, "release"); // или 'release'
