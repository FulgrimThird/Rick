const axios = require('axios');
const { Pool } = require('pg');


const MAX_CONNECTIONS = 50;  //Соединений в пуле
const PAGES_AT_ONCE = 10;   //Страниц за раз

const pool = new Pool({
   user: "Rick",
   host: "194.120.116.148",
   database: "Rick",
  password: "qweasdzxc",
  max:  MAX_CONNECTIONS,                     
});

const tableName = 'tt111';

const createTableQuery = `
  CREATE TABLE IF NOT EXISTS ${tableName} (
    id SERIAL PRIMARY KEY,
    name TEXT,
    data JSONB
  );
`;

const insertCharacters = async (characters) => {
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
   // values.forEach(([name]) => console.log(`Персонаж добавлен в базу: ${name}`));
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Ошибка при вставке данных:', err.message);
  } finally {
    client.release();
  }
};

const fetchPage = async (page) => {
  try {
    const response = await axios.get(`https://rickandmortyapi.com/api/character/?page=${page}`);
    const characters = response.data.results;
    await insertCharacters(characters);
  } catch (error) {
    console.error(`Ошибка при загрузке страницы ${page}:`, error.message);
  }
};

const fetchCharacters = async () => {
  try {
    const initialResponse = await axios.get(`https://rickandmortyapi.com/api/character/`);
    const totalPages = initialResponse.data.info.pages;

    let page = 1;
    const pageGroups = [];

    while (page <= totalPages) {
      const tasks = [];
      for (let i = 0; i < PAGES_AT_ONCE && page <= totalPages; i++, page++) {
        tasks.push(fetchPage(page));
      }
      pageGroups.push(Promise.all(tasks));
    }

    const startTime = Date.now();
    await Promise.all(pageGroups);
    const endTime = Date.now();

    console.log(`Данные успешно загружены в таблицу ${tableName}`);
    console.log(`Время выполнения: ${(endTime - startTime) / 1000} секунд`);
  } catch (error) {
    console.error('Ошибка при загрузке данных:', error.message);
  } finally {
    pool.end((err) => {
      if (err) {
        console.error('Ошибка при закрытии соединения с базой данных:', err.message);
      } else {
        console.log('Соединение с базой данных успешно закрыто.');
      }
    });
  }
};

const main = async () => {
  const client = await pool.connect();
  try {
    await client.query(createTableQuery);
    console.log('Таблица успешно создана или уже существует.');
    await fetchCharacters();
  } catch (err) {
    console.error('Ошибка при создании таблицы:', err.message);
  } finally {
    client.release();
  }
};

main();