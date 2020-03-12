import generate from '@babel/generator';
import * as t from '@babel/types';

const capitaliseFirstLetter = (str: string): string => {
  return str.charAt(0).toUpperCase() + str.slice(1);
};

class Context {
  namedTypesByName = new Map<string, t.FlowType>();
  allTypes: Array<[ReadonlyArray<string>, t.FlowType]> = [];
  addName(name: string, type: t.FlowType) {
    if (this.namedTypesByName.has(name)) {
      throw new Error(`The named type ${name} has already been declared`);
    }
    this.namedTypesByName.set(name, type);
  }
  getName(name: string): t.FlowType {
    const namedType = this.namedTypesByName.get(name);
    if (namedType == null) {
      throw new Error(`Named type ${name} has not been declared`);
    }
    return namedType;
  }

  addType(names: ReadonlyArray<string>, newType: t.FlowType) {
    this.allTypes.push([names, newType]);
  }
}

enum AvroPrimitiveType {
  NULL = 'null',
  BOOLEAN = 'boolean',
  INT = 'int',
  LONG = 'long',
  FLOAT = 'float',
  DOUBLE = 'double',
  BYTES = 'bytes',
  STRING = 'string',
}

const AllAvroPrimitiveTypes = new Set(Object.values(AvroPrimitiveType));

type AvroPrimitive = AvroPrimitiveType | { type: AvroPrimitiveType };

type CustomType = string;

type AvroType = AvroPrimitive | AvroComplex | CustomType;

type AvroComplex = AvroRecord | AvroArray | AvroUnion | AvroEnum;

interface AvroEnum {
  type: 'enum';
  name: string;
  symbols: Array<string>;
}

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

type Parser<T> = (
  a: T,
  names: ReadonlyArray<string>,
  context: Context
) => t.FlowType;

export const parseFile = (name: string, avscText: string): string => {
  const avro: AvroType = JSON.parse(avscText);
  const context = new Context();
  parseAvroType(avro, [name], context);
  const file = t.file(
    t.program(
      context.allTypes.map(([names, flowType]) =>
        t.exportNamedDeclaration(
          t.typeAlias(
            t.identifier(
              names
                .map(capitaliseFirstLetter)
                .map(s => s.replace(' ', ''))
                .join('')
            ),
            null,
            flowType
          )
        )
      ),
      undefined,
      'module',
      null
    ),
    null,
    null
  );
  return generate(file).code;
};

const parseAvroRecord: Parser<AvroRecord> = (
  avro,
  names,
  context
): t.FlowType => {
  const record = t.objectTypeAnnotation(
    avro.fields.map(field => {
      const property = t.objectTypeProperty(
        t.stringLiteral(field.name),
        parseAvroType(field.type, [...names, avro.name, field.name], context)
      );
      return property;
    }),
    null,
    null,
    null,
    true
  );
  context.addName(avro.name, record);
  return record;
};

const parseAvroArray: Parser<AvroArray> = (
  avro,
  names,
  context: Context
): t.FlowType => {
  return t.arrayTypeAnnotation(parseAvroType(avro.items, names, context));
};

interface AvroTypeHandlersMap<T> {
  primitive: (_: AvroPrimitiveType) => T;
  array: (_: AvroArray) => T;
  record: (_: AvroRecord) => T;
  union: (_: AvroUnion) => T;
  custom: (_: CustomType) => T;
  enum: (_: AvroEnum) => T;
}

const handleAvroType = <T>(
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
    default:
      // Must be a primitive or custom type
      return handlersMap.primitive(avro.type);
  }
};

const parseAvroEnum: Parser<AvroEnum> = (avro, _, context) => {
  const enumType = t.unionTypeAnnotation(
    avro.symbols.map(symbol => t.stringLiteralTypeAnnotation(symbol))
  );
  context.addName(avro.name, enumType);
  return enumType;
};

const parseAvroType: Parser<AvroType> = (avro, names, context) => {
  return handleAvroType(avro, {
    primitive: parseAvroPrimitiveType,
    union: a => {
      const parsed = parseAvroUnionType(a, names, context);
      context.addType([...names, 'Union'], parsed);
      return parsed;
    },
    record: a => {
      const parsed = parseAvroRecord(a, names, context);
      context.addType([...names, a.name], parsed);
      return parsed;
    },
    array: a => parseAvroArray(a, names, context),
    custom: a => parseAvroCustomType(a, context),
    enum: a => {
      const parsed = parseAvroEnum(a, names, context);
      context.addType([...names, a.name], parsed);
      return parsed;
    },
  });
};

const parseAvroCustomType = (
  typeName: CustomType,
  context: Context
): t.FlowType => {
  const customType = context.getName(typeName);
  if (customType == null) {
    throw new Error(`Could not find definition for custom type ${typeName}`);
  }
  return customType;
};

const parseAvroPrimitiveType = (pt: AvroPrimitiveType): t.FlowType => {
  switch (pt) {
    case AvroPrimitiveType.NULL:
      return t.nullLiteralTypeAnnotation();
    case AvroPrimitiveType.BOOLEAN:
      return t.booleanTypeAnnotation();
    case AvroPrimitiveType.INT:
      return t.numberTypeAnnotation();
    case AvroPrimitiveType.LONG:
      return t.numberTypeAnnotation();
    case AvroPrimitiveType.FLOAT:
      return t.numberTypeAnnotation();
    case AvroPrimitiveType.DOUBLE:
      return t.numberTypeAnnotation();
    case AvroPrimitiveType.BYTES:
      throw new Error("Can't handle bytes");
    case AvroPrimitiveType.STRING:
      return t.stringTypeAnnotation();
  }
};

const parseAvroUnionType: Parser<AvroUnion> = (
  unionTypes,
  names,
  context
): t.FlowType => {
  return t.unionTypeAnnotation(
    unionTypes.map(unionType => {
      const flowType = parseAvroType(unionType, names, context);
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
    custom: s => s,
    ['enum']: s => s.name,
  });
};
