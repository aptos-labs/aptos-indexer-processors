import "reflect-metadata";
import { DataSource } from "typeorm";
import { SnakeNamingStrategy } from "typeorm-naming-strategies";
import { NextVersionToProcess } from "./next_version_to_process";
import { Base } from "./base";

// TODO: Do migrations stuff??, the below function takes migrations as an argument.
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
