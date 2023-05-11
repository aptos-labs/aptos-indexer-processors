import { Entity, PrimaryColumn, Column } from "typeorm";

@Entity()
export class WriteSetChange {
  @PrimaryColumn()
  transactionVersion: number;

  @PrimaryColumn()
  index: number;

  @Column()
  stateKeyHash: string;

  @Column()
  transactionBlockHeight: number;

  @Column()
  type: string;

  @Column()
  address: string;

  @Column("timestamp with time zone")
  insertedAt: number;
}
