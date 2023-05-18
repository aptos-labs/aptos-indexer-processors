import { Column, Entity, PrimaryColumn } from "typeorm";
import { BaseEntity } from "@/utils/common_models/Base";

@Entity("next_version_to_process")
export class NextVersionToProcess extends BaseEntity {
  @PrimaryColumn()
  indexerName: string;

  @Column({ type: "bigint" })
  nextVersion: string;
}
