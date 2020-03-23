import generate from '@babel/generator';
import * as t from '@babel/types';

const validateAvroCustomName = (string: string) => {
  const regex = /^[A-Za-z_][A-Za-z0-9_]*$/;
  if (regex.test(string) === false) {
    throw new Error(`'${string}' is not a valid avro name`);
  }
};

const createTypeName = (names: ReadonlyArray<string>): string => {
  return names
    .map(name =>
      name
        .replace(' ', '_')
        .split('_')
        .map(capitaliseFirstLetter)
        .join('')
    )
    .map(capitaliseFirstLetter)
    .map(s => s.replace(' ', ''))
    .join('_');
};

const capitaliseFirstLetter = (str: string): string => {
  return str.charAt(0).toUpperCase() + str.slice(1);
};

class Context {
  wrapPrimitives: boolean;
  constructor(wrapPrimitives: boolean) {
    this.wrapPrimitives = wrapPrimitives;
  }
  namedTypesByName = new Map<string, t.FlowType>();
  allTypes: Array<[string, t.FlowType]> = [];
  addCustomType(name: string, type: t.FlowType) {
    if (this.namedTypesByName.has(name)) {
      throw new Error(`The named type ${name} has already been declared`);
    }
    this.namedTypesByName.set(name, type);
  }

  addType(name: string, newType: t.FlowType) {
    this.allTypes.push([name, newType]);
  }

  getFlowNameForAvroCustomName(customAvroName: string): string {
    const customTypeNode = this.namedTypesByName.get(customAvroName);
    if (customTypeNode == null) {
      throw new Error(`Named type ${customAvroName} has not been declared`);
    }
    const flowNameAndNode = this.allTypes.find(
      ([_, node]) => node === customTypeNode
    );
    if (flowNameAndNode == null) {
      throw new Error('Logic error – should be impossible');
    }
    const [flowName] = flowNameAndNode;
    return flowName;
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

type AvroComplex = AvroRecord | AvroArray | AvroUnion | AvroEnum | AvroMap;

interface AvroMap {
  type: 'map';
  values: AvroType;
}

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

type Parser<T, ResultType extends t.FlowType = t.FlowType> = (
  a: T,
  parentNames: ReadonlyArray<string>,
  context: Context,
  nameOverride?: null | string
) => ResultType;

interface Options {
  wrapPrimitives: boolean;
}

export const parseFile = (avscText: string, options?: Options): string => {
  const avro: AvroType = JSON.parse(avscText);
  const context = new Context(options?.wrapPrimitives ?? true);
  parseAvroType(avro, [], context);
  const file = t.file(
    t.program(
      context.allTypes.map(([names, flowType]) =>
        t.exportNamedDeclaration(
          t.typeAlias(t.identifier(names), null, flowType)
        )
      ),
      undefined,
      'module',
      null
    ),
    null,
    null
  );
  // lol
  return '// @flow\n\n' + generate(file).code;
};

const parseAvroRecord: Parser<AvroRecord> = (
  avro,
  parentNames,
  context,
  nameOverride
): t.FlowType => {
  validateAvroCustomName(avro.name);
  const name = nameOverride ?? avro.name;
  const record = t.objectTypeAnnotation(
    avro.fields.map(field => {
      const property = t.objectTypeProperty(
        t.stringLiteral(field.name),
        parseAvroType(field.type, [...parentNames, name, field.name], context)
      );
      return property;
    }),
    null,
    null,
    null,
    true
  );
  context.addCustomType(avro.name, record);
  return record;
};

const parseAvroArray: Parser<AvroArray> = (
  avro,
  names,
  context
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
  map: (_: AvroMap) => T;
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
    case 'map':
      return handlersMap.map(avro);
    default:
      // Must be a primitive or custom type
      return handlersMap.primitive(avro.type);
  }
};

const parseAvroEnum: Parser<AvroEnum> = (avro, _, context) => {
  validateAvroCustomName(avro.name);
  const enumType = t.unionTypeAnnotation(
    avro.symbols.map(symbol => t.stringLiteralTypeAnnotation(symbol))
  );
  context.addCustomType(avro.name, enumType);
  return enumType;
};

const parseAvroType = (
  avro: AvroType,
  names: ReadonlyArray<string>,
  context: Context
) => {
  return handleAvroType(avro, {
    primitive: parseAvroPrimitiveType,
    union: a => {
      const typeName = names.length > 0 ? createTypeName(names) : 'Union';
      const parsed = parseAvroUnionType(a, names, context);

      const shouldInline =
        names.length > 0 &&
        parsed.types.length === 2 &&
        parsed.types.some(
          branch => branch.type === t.nullLiteralTypeAnnotation().type
        );
      if (shouldInline) {
        return parsed;
      } else {
        context.addType(typeName, parsed);
        return t.genericTypeAnnotation(t.identifier(typeName));
      }
    },
    record: a => {
      const parsed = parseAvroRecord(a, [], context);
      context.addType(a.name, parsed);
      return t.genericTypeAnnotation(t.identifier(a.name));
    },
    array: a => parseAvroArray(a, names, context),
    custom: a => parseAvroCustomType(a, context),
    enum: a => {
      const parsed = parseAvroEnum(a, [], context);
      context.addType(a.name, parsed);
      return t.genericTypeAnnotation(t.identifier(a.name));
    },
    map: a => {
      const typeName = createTypeName([...names, 'Map']);
      const parsed = parseAvroMapType(a, names, context);
      context.addType(typeName, parsed);
      return t.genericTypeAnnotation(t.identifier(typeName));
    },
  });
};

const parseAvroMapType: Parser<AvroMap> = (avro, names, context) => {
  return t.objectTypeAnnotation(
    [],
    [
      t.objectTypeIndexer(
        null,
        t.stringTypeAnnotation(),
        parseAvroType(avro.values, names, context)
      ),
    ]
  );
};

const parseAvroCustomType = (
  typeName: CustomType,
  context: Context
): t.FlowType => {
  return t.genericTypeAnnotation(
    t.identifier(context.getFlowNameForAvroCustomName(typeName))
  );
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

const parseAvroUnionType: Parser<AvroUnion, t.UnionTypeAnnotation> = (
  unionTypes,
  names,
  context
) => {
  // This is necessary to deduplicate non-wrapped primitives
  const memberTypeAnnotations: {
    tagless: Array<t.FlowType>;
    tagged: Array<t.FlowType>;
  } = {
    tagless: [],
    tagged: [],
  };
  for (const unionType of unionTypes) {
    const flowType = parseAvroType(unionType, names, context);
    const tag = tagForUnionBranch(unionType, context.wrapPrimitives);
    if (tag == null) {
      memberTypeAnnotations.tagless.push(flowType);
    } else {
      memberTypeAnnotations.tagged.push(
        t.objectTypeAnnotation(
          [t.objectTypeProperty(t.identifier(tag), flowType)],
          null,
          null,
          null,
          true
        )
      );
    }
  }

  let uniqueTagless = Array.from(
    new Map(memberTypeAnnotations.tagless.map(ann => [ann.type, ann])).values()
  );
  let annotations = uniqueTagless.concat(memberTypeAnnotations.tagged);

  return t.unionTypeAnnotation(annotations);
};

const tagForUnionBranch = (
  avro: AvroType,
  wrapPrimitives: boolean
): null | string => {
  return handleAvroType<null | string>(avro, {
    primitive: primitive => {
      if (primitive === 'null' || !wrapPrimitives) {
        return null;
      } else {
        return primitive;
      }
    },
    union: () => 'union',
    record: r => r.name,
    array: () => 'array',
    custom: s => s,
    enum: s => s.name,
    map: () => 'map',
  });
};
