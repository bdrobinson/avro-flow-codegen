import generate from '@babel/generator';
import * as t from '@babel/types';

type AvroPrimitiveType =
  | 'null'
  | 'boolean'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'bytes'
  | 'string';

type AvroPrimitive = AvroPrimitiveType | { type: AvroPrimitiveType };

type AvroType = AvroPrimitive | AvroComplex;

type AvroComplex = AvroRecord | AvroArray | AvroUnion;

interface AvroRecord {
  type: 'record';
  name: string;
  fields: Array<AvroRecordField>;
}
interface AvroRecordField {
  name: string;
  type: AvroType;
}

interface AvroArray {
  type: 'array';
  items: AvroType;
}
type AvroUnion = Array<AvroType>;

export const generateFile = (name: string, avscText: string): string => {
  const avscJson = JSON.parse(avscText);
  if (avscJson.type !== 'record') {
    throw new Error('Expected top level to be record');
  }
  const topRecord: AvroRecord = avscJson;
  const ast = t.typeAlias(t.identifier(name), null, parseAvroRecord(topRecord));
  return generate(ast).code;
};

const parseAvroRecord = (avro: AvroRecord): t.FlowType => {
  return t.objectTypeAnnotation(
    avro.fields.map(field => {
      return t.objectTypeProperty(
        t.stringLiteral(field.name),
        parseAvroType(field.type)
      );
    }),
    null,
    null,
    null,
    true
  );
};

const parseAvroArray = (avro: AvroArray): t.FlowType => {
  return t.arrayTypeAnnotation(parseAvroType(avro.items));
};

interface AvroTypeHandlersMap<T> {
  primitive: (_: AvroPrimitiveType) => T;
  array: (_: AvroArray) => T;
  record: (_: AvroRecord) => T;
  union: (_: AvroUnion) => T;
}

const handleAvroType = <T>(
  avro: AvroType,
  handlersMap: AvroTypeHandlersMap<T>
): T => {
  if (typeof avro === 'string') {
    return handlersMap.primitive(avro);
  } else if (Array.isArray(avro)) {
    return handlersMap.union(avro);
  }
  switch (avro.type) {
    case 'record':
      return handlersMap.record(avro);
    case 'array':
      return handlersMap.array(avro);
    default:
      // Must be a primitive or custom type
      return handlersMap.primitive(avro.type);
  }
};

const parseAvroType = (avro: AvroType): t.FlowType => {
  return handleAvroType(avro, {
    primitive: parseAvroPrimitiveType,
    union: parseAvroUnionType,
    record: parseAvroRecord,
    array: parseAvroArray,
  });
};
const parseAvroPrimitiveType = (pt: AvroPrimitiveType): t.FlowType => {
  switch (pt) {
    case 'null':
      return t.nullLiteralTypeAnnotation();
    case 'boolean':
      return t.booleanTypeAnnotation();
    case 'int':
      return t.numberTypeAnnotation();
    case 'long':
      return t.numberTypeAnnotation();
    case 'float':
      return t.numberTypeAnnotation();
    case 'double':
      return t.numberTypeAnnotation();
    case 'bytes':
      throw new Error("Can't handle bytes");
    case 'string':
      return t.stringTypeAnnotation();
  }
};

const parseAvroUnionType = (unionTypes: AvroUnion): t.FlowType => {
  return t.unionTypeAnnotation(
    unionTypes.map(unionType => {
      const flowType = parseAvroType(unionType);
      const tag = tagForUnionBranch(unionType);
      if (tag == null) {
        return flowType;
      }
      return t.objectTypeAnnotation(
        [t.objectTypeProperty(t.identifier(tag), flowType)],
        null,
        null,
        null,
        true
      );
    })
  );
};

const tagForUnionBranch = (avro: AvroType): null | string => {
  return handleAvroType<null | string>(avro, {
    primitive: primitive => {
      if (primitive === 'null') {
        return null;
      } else {
        return primitive;
      }
    },
    union: () => {
      throw new Error('is this even possible?');
    },
    record: r => r.name,
    array: () => {
      throw new Error('is this even possible?');
    },
  });
};
