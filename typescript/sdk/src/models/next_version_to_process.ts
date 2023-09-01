import { Column, Entity, PrimaryColumn } from "typeorm";
import { Base } from "./base";

@Entity("next_version_to_process")
export class NextVersionToProcess extends Base {
  @PrimaryColumn()
  indexerName!: string;

  @Column({ type: "bigint" })
  nextVersion!: string;
}

export function createNextVersionToProcess({
  indexerName,
  version,
}: {
  indexerName: string;
  version: bigint;
}) {
  const nextVersionToProcess = new NextVersionToProcess();
  nextVersionToProcess.indexerName = indexerName;
  nextVersionToProcess.nextVersion = version.toString();
  return nextVersionToProcess;
}
