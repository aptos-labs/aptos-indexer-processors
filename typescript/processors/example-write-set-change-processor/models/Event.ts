import { Column, Entity, PrimaryColumn } from "typeorm";
import { BaseEntity } from "@/utils/common_models/Base";

@Entity("events")
export class Event extends BaseEntity {
  @PrimaryColumn({ type: "bigint" })
  sequenceNumber: string;

  @PrimaryColumn({ type: "bigint" })
  creationNumber: string;

  @PrimaryColumn()
  accountAddress: string;

  @Column({ type: "bigint" })
  transactionVersion: string;

  @Column({ type: "bigint" })
  transactionBlockHeight: string;

  @Column()
  type: string;

  @Column()
  data: string;

  @Column({ type: "bigint" })
  eventIndex: string;

  @Column({ type: "timestamptz", nullable: true })
  inserted_at: Date;
}
