export enum AvroPrimitiveType {
  NULL = 'null',
  BOOLEAN = 'boolean',
  INT = 'int',
  LONG = 'long',
  FLOAT = 'float',
  DOUBLE = 'double',
  BYTES = 'bytes',
  STRING = 'string',
}

export const AllAvroPrimitiveTypes = new Set(Object.values(AvroPrimitiveType));

export type AvroPrimitive = AvroPrimitiveType | { type: AvroPrimitiveType };

export type CustomType = string;

export type AvroType = AvroPrimitive | AvroComplex | CustomType;

export type AvroComplex =
  | AvroRecord
  | AvroArray
  | AvroUnion
  | AvroEnum
  | AvroMap;

export interface AvroMap {
  type: 'map';
  values: AvroType;
}

export interface AvroEnum {
  type: 'enum';
  name: string;
  symbols: Array<string>;
}

export interface AvroRecord {
  type: 'record';
  name: string;
  fields: Array<AvroRecordField>;
}
export interface AvroRecordField {
  name: string;
  type: AvroType;
}

export interface AvroArray {
  type: 'array';
  items: AvroType;
}
export type AvroUnion = Array<AvroType>;

export interface AvroTypeHandlersMap<T> {
  primitive: (_: AvroPrimitiveType) => T;
  array: (_: AvroArray) => T;
  record: (_: AvroRecord) => T;
  union: (_: AvroUnion) => T;
  custom: (_: CustomType) => T;
  enum: (_: AvroEnum) => T;
  map: (_: AvroMap) => T;
}

export const handleAvroType = <T>(
  avro: AvroType,
  handlersMap: AvroTypeHandlersMap<T>
): T => {
  if (typeof avro === 'string') {
    // @ts-ignore
    if (AllAvroPrimitiveTypes.has(avro)) {
      // @ts-ignore
      const prim: AvroPrimitiveType = avro;
      return handlersMap.primitive(prim);
    } else {
      // Must be a custom type
      return handlersMap.custom(avro);
    }
  } else if (Array.isArray(avro)) {
    return handlersMap.union(avro);
  }
  switch (avro.type) {
    case 'record':
      return handlersMap.record(avro);
    case 'array':
      return handlersMap.array(avro);
    case 'enum':
      return handlersMap.enum(avro);
    case 'map':
      return handlersMap.map(avro);
    default:
      // Must be a primitive or custom type
      return handlersMap.primitive(avro.type);
  }
};
