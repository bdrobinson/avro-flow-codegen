import * as t from '@babel/types';

import * as a from './avro';

interface Context {
  wrapPrimitives: boolean;
  isValidCustomType(typeName: string): boolean;
}

export interface Transformer<T> {
  transform(avro: a.AvroType): T;
}

abstract class TransformerImpl<T> implements Transformer<T> {
  abstract primitive(avro: a.AvroPrimitiveType): T;
  abstract array(avro: a.AvroArray): T;
  abstract record(avro: a.AvroRecord): T;
  abstract union(avro: a.AvroUnion): T;
  abstract custom(avro: a.CustomType): T;
  abstract enum(avro: a.AvroEnum): T;
  abstract map(avro: a.AvroMap): T;
  transform(avro: a.AvroType): T {
    return a.handleAvroType(avro, {
      primitive: a => this.primitive(a),
      union: a => this.union(a),
      record: a => this.record(a),
      array: a => this.array(a),
      custom: a => this.custom(a),
      enum: a => this.enum(a),
      map: a => this.map(a),
    });
  }
}

export const createFlowTransformer = (
  context: Context
): Transformer<t.FlowType> => {
  return new FlowTransformer(context);
};

class FlowTransformer extends TransformerImpl<t.FlowType> {
  context: Context;
  constructor(context: Context) {
    super();
    this.context = context;
  }
  primitive(pt: a.AvroPrimitiveType) {
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
  }
  array(avro: a.AvroArray) {
    return t.arrayTypeAnnotation(this.transform(avro.items));
  }
  record(avro: a.AvroRecord) {
    const record = t.objectTypeAnnotation(
      avro.fields.map(field => {
        const property = t.objectTypeProperty(
          t.stringLiteral(field.name),
          this.transform(field.type)
        );
        return property;
      }),
      null,
      null,
      null,
      true
    );
    return record;
  }
  union(unionTypes: a.AvroUnion) {
    // This is necessary to deduplicate non-wrapped primitives
    const memberTypeAnnotations: {
      tagless: Array<t.FlowType>;
      tagged: Array<t.FlowType>;
    } = {
      tagless: [],
      tagged: [],
    };
    for (const unionType of unionTypes) {
      const flowType = this.transform(unionType);
      const tag = tagForUnionBranch(unionType, this.context.wrapPrimitives);
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
      new Map(
        memberTypeAnnotations.tagless.map(ann => [ann.type, ann])
      ).values()
    );
    let annotations = uniqueTagless.concat(memberTypeAnnotations.tagged);

    return t.unionTypeAnnotation(annotations);
  }
  custom(typeName: a.CustomType) {
    if (this.context.isValidCustomType(typeName) === false) {
      throw new Error(`${typeName} is not a valid custom type name.`);
    }
    return t.genericTypeAnnotation(t.identifier(typeName));
  }
  enum(avro: a.AvroEnum) {
    const enumType = t.unionTypeAnnotation(
      avro.symbols.map(symbol => t.stringLiteralTypeAnnotation(symbol))
    );
    return enumType;
  }
  map(avro: a.AvroMap) {
    return t.objectTypeAnnotation(
      [],
      [
        t.objectTypeIndexer(
          null,
          t.stringTypeAnnotation(),
          this.transform(avro.values)
        ),
      ]
    );
  }
}

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
