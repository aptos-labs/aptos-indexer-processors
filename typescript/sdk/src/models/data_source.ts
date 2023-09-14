import "reflect-metadata";
import { DataSource } from "typeorm";
import { SnakeNamingStrategy } from "typeorm-naming-strategies";
import { NextVersionToProcess } from "./next_version_to_process";
import { Base } from "./base";

/**
 * This function returns a DataSource, something akin to a DB connection object.
 * When dataSource.initialize() is called it runs any necessary DB migrations.
 */
export function createDataSource({
  host,
  port,
  username,
  password,
  database,
  enableSSL,
  additionalEntities,
}: {
  host: string;
  port: number;
  username?: string;
  password?: string;
  database: string;
  enableSSL: boolean;
  additionalEntities?: typeof Base[];
}) {
  const entities = [NextVersionToProcess, ...(additionalEntities || [])];
  return new DataSource({
    namingStrategy: new SnakeNamingStrategy(),
    type: "postgres",
    host,
    port,
    username,
    password,
    database,
    synchronize: true,
    logging: false,
    entities,
    subscribers: [],
    ssl: enableSSL,
  });
}
