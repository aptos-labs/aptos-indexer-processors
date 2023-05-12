import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { SnakeNamingStrategy } from 'typeorm-naming-strategies';
import { Event } from './entity/Event';
import { NextVersionToProcess } from './entity/NextVersionToProcess';

export function createDataSource(
  host: string,
  port: number,
  username: string | undefined,
  password: string | undefined,
  database: string,
  enableSSL: boolean
) {
  return new DataSource({
    namingStrategy: new SnakeNamingStrategy(),
    type: 'postgres',
    host,
    port,
    username,
    password,
    database,
    synchronize: true,
    logging: false,
    entities: [Event, NextVersionToProcess],
    migrations: [__dirname + '/migrations/**/*{.ts,.js}'],
    subscribers: [],
    ssl: enableSSL,
    migrationsRun: false,
  });
}
