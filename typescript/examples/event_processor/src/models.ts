import { Base } from "@aptos-labs/aptos-processor-sdk";
import { Column, Entity, PrimaryColumn } from "typeorm";

@Entity("events")
export class Event extends Base {
  @PrimaryColumn({ type: "bigint" })
  transactionVersion!: string;

  // This is the order of the event in the events list in the trannsaction.
  @PrimaryColumn({ type: "bigint" })
  eventIndex!: string;

  @Column({ type: "bigint" })
  creationNumber!: string;

  @Column()
  accountAddress!: string;

  @Column({ type: "bigint" })
  sequenceNumber!: string;

  @Column()
  type!: string;

  @Column({ type: "bigint" })
  transactionBlockHeight!: string;

  @Column()
  data!: string;

  @Column({ type: "timestamptz", nullable: true })
  inserted_at!: Date;
}
