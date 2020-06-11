import * as t from '@babel/types';

import * as a from './avro';

interface Context {
  getFlowNameForAvroCustomName(name: string): string;
  wrapPrimitives: boolean;
}

type Parser<T, ResultType> = (a: T, context: Context) => ResultType;

type FlowParser<T> = Parser<T, t.FlowType>;

const parseAvroRecord: FlowParser<a.AvroRecord> = (
  avro,
  context
): t.FlowType => {
  const record = t.objectTypeAnnotation(
    avro.fields.map(field => {
      const property = t.objectTypeProperty(
        t.stringLiteral(field.name),
        parseAvroType(field.type, context)
      );
      return property;
    }),
    null,
    null,
    null,
    true
  );
  return record;
};

const parseAvroArray: FlowParser<a.AvroArray> = (avro, context): t.FlowType => {
  return t.arrayTypeAnnotation(parseAvroType(avro.items, context));
};

const parseAvroEnum: FlowParser<a.AvroEnum> = avro => {
  const enumType = t.unionTypeAnnotation(
    avro.symbols.map(symbol => t.stringLiteralTypeAnnotation(symbol))
  );
  return enumType;
};

export const parseAvroType: FlowParser<a.AvroType> = (avro, context) => {
  return a.handleAvroType(avro, {
    primitive: parseAvroPrimitiveType,
    union: a => {
      return parseAvroUnionType(a, context);
    },
    record: a => {
      return parseAvroRecord(a, context);
    },
    array: a => parseAvroArray(a, context),
    custom: a => parseAvroCustomType(a, context),
    enum: a => {
      return parseAvroEnum(a, context);
    },
    map: a => {
      return parseAvroMapType(a, context);
    },
  });
};

const parseAvroMapType: FlowParser<a.AvroMap> = (avro, context) => {
  return t.objectTypeAnnotation(
    [],
    [
      t.objectTypeIndexer(
        null,
        t.stringTypeAnnotation(),
        parseAvroType(avro.values, context)
      ),
    ]
  );
};

const parseAvroCustomType = (
  typeName: a.CustomType,
  context: Context
): t.FlowType => {
  return t.genericTypeAnnotation(
    t.identifier(context.getFlowNameForAvroCustomName(typeName))
  );
};

const parseAvroPrimitiveType = (pt: a.AvroPrimitiveType): t.FlowType => {
  switch (pt) {
    case a.AvroPrimitiveType.NULL:
      return t.nullLiteralTypeAnnotation();
    case a.AvroPrimitiveType.BOOLEAN:
      return t.booleanTypeAnnotation();
    case a.AvroPrimitiveType.INT:
      return t.numberTypeAnnotation();
    case a.AvroPrimitiveType.LONG:
      return t.numberTypeAnnotation();
    case a.AvroPrimitiveType.FLOAT:
      return t.numberTypeAnnotation();
    case a.AvroPrimitiveType.DOUBLE:
      return t.numberTypeAnnotation();
    case a.AvroPrimitiveType.BYTES:
      throw new Error("Can't handle bytes");
    case a.AvroPrimitiveType.STRING:
      return t.stringTypeAnnotation();
  }
};

const parseAvroUnionType: FlowParser<a.AvroUnion> = (
  unionTypes,
  context
): t.UnionTypeAnnotation => {
  // This is necessary to deduplicate non-wrapped primitives
  const memberTypeAnnotations: {
    tagless: Array<t.FlowType>;
    tagged: Array<t.FlowType>;
  } = {
    tagless: [],
    tagged: [],
  };
  for (const unionType of unionTypes) {
    const flowType = parseAvroType(unionType, context);
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
  avro: a.AvroType,
  wrapPrimitives: boolean
): null | string => {
  return a.handleAvroType<null | string>(avro, {
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
