import {
  CreateDateColumn,
  UpdateDateColumn,
  DeleteDateColumn,
  Index,
  BaseEntity,
} from "typeorm";

export abstract class Base extends BaseEntity {
  @CreateDateColumn()
  createdAt!: Date;

  @UpdateDateColumn()
  updatedAt!: Date;

  @Index()
  @DeleteDateColumn()
  deletedAt!: Date;
}
