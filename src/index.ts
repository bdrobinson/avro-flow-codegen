import generate from '@babel/generator';
import * as t from '@babel/types';

import * as a from './avro';

import * as transformers from './transformers';

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
  namedTypesByName = new Map<string, a.AvroType>();
  allTypes: Array<[string, a.AvroType]> = [];
  addCustomType(name: string, type: a.AvroType) {
    if (this.namedTypesByName.has(name)) {
      throw new Error(`The named type ${name} has already been declared`);
    }
    this.namedTypesByName.set(name, type);
  }

  addType(name: string, newType: a.AvroType) {
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

type Visitor<T> = (
  a: T,
  parentNames: ReadonlyArray<string>,
  context: Context,
  nameOverride?: null | string
) => void;

interface Options {
  wrapPrimitives: boolean;
}

export const parseFile = (avscText: string, options?: Options): string => {
  const avro: a.AvroType = JSON.parse(avscText);
  const context = new Context(options?.wrapPrimitives ?? true);
  visitAvroType(avro, [], context);
  const file = t.file(
    t.program(
      context.allTypes.map(([names, avroType]) =>
        t.exportNamedDeclaration(
          t.typeAlias(
            t.identifier(names),
            null,
            transformers.parseAvroType(avroType, context)
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
  // lol
  return '// @flow\n\n' + generate(file).code;
};

const visitAvroRecord: Visitor<a.AvroRecord> = (
  avro,
  parentNames,
  context,
  nameOverride
) => {
  validateAvroCustomName(avro.name);
  const name = nameOverride ?? avro.name;
  avro.fields.forEach(field => {
    visitAvroType(field.type, [...parentNames, name, field.name], context);
  });
  context.addCustomType(avro.name, avro);
};

const visitAvroArray: Visitor<a.AvroArray> = (avro, names, context) => {
  visitAvroType(avro.items, names, context);
};

const isAvroTypeNull = (avro: a.AvroType): boolean => {
  return a.handleAvroType<boolean>(avro, {
    primitive: primitive => {
      return primitive === a.AvroPrimitiveType.NULL;
    },
    array: () => false,
    record: () => false,
    union: () => false,
    custom: () => false,
    enum: () => false,
    map: () => false,
  });
};

const visitAvroEnum: Visitor<a.AvroEnum> = (avro, _, context) => {
  validateAvroCustomName(avro.name);
  context.addCustomType(avro.name, avro);
};

const visitAvroType = (
  avro: a.AvroType,
  names: ReadonlyArray<string>,
  context: Context
) => {
  return a.handleAvroType<void>(avro, {
    primitive: () => {},
    union: a => {
      visitAvroUnionType(a, names, context);
      const typeName = names.length > 0 ? createTypeName(names) : 'Union';

      const shouldInline =
        names.length > 0 && a.length === 2 && a.some(isAvroTypeNull);

      if (shouldInline) {
        //
      } else {
        context.addType(typeName, a);
      }
    },
    record: a => {
      visitAvroRecord(a, [], context);
      context.addType(a.name, a);
    },
    array: a => visitAvroArray(a, names, context),
    custom: () => {},
    enum: a => {
      visitAvroEnum(a, [], context);
      context.addType(a.name, a);
    },
    map: a => {
      const typeName = createTypeName([...names, 'Map']);
      visitAvroMapType(a, names, context);
      context.addType(typeName, a);
    },
  });
};

const visitAvroMapType: Visitor<a.AvroMap> = (avro, names, context) => {
  visitAvroType(avro.values, names, context);
};

const visitAvroUnionType: Visitor<a.AvroUnion> = (
  unionTypes,
  names,
  context
) => {
  for (const unionType of unionTypes) {
    visitAvroType(unionType, names, context);
  }
};
