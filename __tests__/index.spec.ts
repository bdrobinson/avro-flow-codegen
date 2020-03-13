import prettier from 'prettier';

import { parseFile } from '../src';

const format = (file: string): string => {
  return prettier.format(file, { parser: 'babel' });
};

const assert = (schema: {}, expectedFile: string) => {
  const parsed = parseFile(JSON.stringify(schema));
  expect(format(parsed)).toBe(format(expectedFile));
};

test('simple schema works', () => {
  const schema = {
    type: 'record',
    name: 'Person',
    fields: [
      { name: 'age', type: 'int' },
      {
        name: 'friends',
        type: {
          type: 'array',
          items: 'string',
        },
      },
      { name: 'spouse_name', type: ['null', 'string'] },
      {
        name: 'favourite_pet',
        type: {
          type: 'record',
          name: 'Pet',
          fields: [{ name: 'species', type: 'string' }],
        },
      },
      { name: 'other_pets', type: { type: 'array', items: 'Pet' } },
    ],
  };
  const expected = `
  // @flow

  export type Person_SpouseName = null | {|
    string: string
    |}
    export type Pet = {|
      species: string
    |}
    export type Person = {|
        age: number,
        friends: string[],
        spouse_name: Person_SpouseName,
        favourite_pet: Pet,
        other_pets: Pet[]
    |}
  `;
  assert(schema, expected);
});

test('rejects invalid avro names', () => {
  const badRecordName = {
    type: 'record',
    fields: [],
    name: 'Some name',
  };
  expect(() => parseFile(JSON.stringify(badRecordName))).toThrow(
    new Error("'Some name' is not a valid avro name")
  );

  const badEnumName = {
    type: 'enum',
    symbols: [],
    name: '9Enum',
  };
  expect(() => parseFile(JSON.stringify(badEnumName))).toThrow(
    new Error("'9Enum' is not a valid avro name")
  );
});

test('supports top level unions', () => {
  const schema = [
    {
      type: 'record',
      name: 'Person',
      fields: [{ name: 'age', type: 'int' }],
    },
    {
      type: 'record',
      name: 'Animal',
      fields: [{ name: 'species', type: 'string' }],
    },
    'string',
  ];
  const expected = `
    // @flow

    export type Person = {|
      age: number,
    |}
    export type Animal = {|
      species: string
      |}
    export type Union =
      |{|
        Person: Person
        |}
      | {|
        Animal: Animal
        |}
      | {|
        string: string
        |}
  `;
  assert(schema, expected);
});
