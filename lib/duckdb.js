import {json} from "micro";
import duckdb from "duckdb";

const {Database} = duckdb;

export default (url) => {
  const database = new Database(url);

  return async function query(req, res) {
    const {sql, params = []} = await json(req);
    const client = await database.connect();

    const statement = await new Promise((resolve, reject) =>
      client.prepare(sql, (err, data) => {
        if (err) reject(err);

        resolve(data);
      }),
    );

    const columns = statement.columns();
    const result = await statement.stream(...params);

    const items = [];
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const chunk = await result.nextChunk();
      if (!chunk) {
        break;
      }

      items.push(...chunk);
    }

    const schema = {
      type: "array",
      items: {
        type: "object",
        properties: columns.reduce(
          (schema, {name, type}) => (
            (schema[name] = dataTypeSchema(type)), schema
          ),
          {},
        ),
      },
    };

    res.end(
      JSON.stringify(
        {
          data: items,
          schema,
        },
        (key, value) => (typeof value === "bigint" ? value.toString() : value), // return everything else unchanged
      ),
    );
  };
};

const array = ["null", "array"],
  boolean = ["null", "boolean"],
  integer = ["null", "integer"],
  number = ["null", "number"],
  object = ["null", "object"],
  string = ["null", "string"];

function dataTypeSchema(type) {
  switch (type.id) {
    case "BIGINT":
    case "HUGEINT":
    case "UBIGINT":
      return {type: string, bigint: true};
    case "TINYINT":
    case "INTEGER":
    case "SMALLINT":
    case "UINTEGER":
    case "USMALLINT":
    case "UTINYINT":
      return {type: integer};
    case "REAL":
    case "DOUBLE":
      return {type: number};
    case "BOOLEAN":
      return {type: boolean};
    case "TIMESTAMP WITH TIME ZONE":
    case "TIMESTAMP":
    case "TIME":
    case "DATE":
      return {type: string, date: true};
    case "INTERVAL":
      return {type: object};
    case "LIST":
      return {type: array, items: dataTypeSchema(type.child)};
    case "VARCHAR":
    default:
      return {type: string};
  }
}
