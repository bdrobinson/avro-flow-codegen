import prettier from 'prettier';

import { parseFile } from '../src';
import * as a from '../src/avro';

const format = (file: string): string => {
  return prettier.format(file, { parser: 'babel' });
};

const assertSuccess = (
  schema: {},
  expectedFlow: string,
  expectedTs: string,
  wrapPrimitives: boolean = true
) => {
  const flow = parseFile(JSON.stringify(schema), {
    wrapPrimitives,
    target: 'flow',
  });
  expect(format(flow)).toBe(format(expectedFlow));
  const ts = parseFile(JSON.stringify(schema), {
    wrapPrimitives,
    target: 'typescript',
  });
  expect(format(ts)).toBe(format(expectedTs));
};

test('simple schema works', () => {
  const schema: a.AvroType = {
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
      {
        name: 'spouse_name',
        type: [
          'null',
          'string',
          {
            type: 'record',
            name: 'FullName',
            fields: [
              { name: 'first', type: 'string' },
              { name: 'second', type: 'string' },
            ],
          },
        ],
      },
      {
        name: 'favourite_pet',
        type: {
          type: 'record',
          name: 'Pet',
          fields: [{ name: 'species', type: 'string' }],
        },
      },
      { name: 'other_pets', type: { type: 'array', items: 'Pet' } },
      {
        name: 'some_map',
        type: { type: 'map', values: 'string' },
      },
    ],
  };
  const expectedFlow = `
  // @flow

  export type FullName = {|
    first: string,
    second: string
  |}
  export type Person_SpouseName = 
    | null
    | {|
      string: string
      |}
      | {|
        FullName: FullName
      |}
    export type Pet = {|
      species: string
    |}
    export type Person_SomeMap_Map = {
      [string]: string
    }
    export type Person = {|
        age: number,
        friends: string[],
        spouse_name: Person_SpouseName,
        favourite_pet: Pet,
        other_pets: Pet[],
        some_map: Person_SomeMap_Map,
    |}
    export type __Schema = Person
  `;

  const expectedTs = `
  export type FullName = {
    first: string,
    second: string,
  }
  export type Person_SpouseName = 
    | null
    | {
      string: string
      }
      | {
        FullName: FullName
      }
    export type Pet = {
      species: string,
    }
    export type Person_SomeMap_Map = Record<string, string>
    export type Person = {
        age: number,
        friends: string[],
        spouse_name: Person_SpouseName,
        favourite_pet: Pet,
        other_pets: Pet[],
        some_map: Person_SomeMap_Map,
    }
    export type __Schema = Person
  `;
  assertSuccess(schema, expectedFlow, expectedTs);
});

const assertFailure = (schema: string, flowError: Error, tsError: Error) => {
  expect(() => parseFile(schema, { target: 'flow' })).toThrow(flowError);

  expect(() => parseFile(schema, { target: 'typescript' })).toThrow(tsError);
};

test('rejects invalid avro names', () => {
  const badRecordName = {
    type: 'record',
    fields: [],
    name: 'Some name',
  };
  assertFailure(
    JSON.stringify(badRecordName),
    new Error("'Some name' is not a valid avro name"),
    new Error("'Some name' is not a valid avro name")
  );

  const badEnumName = {
    type: 'enum',
    symbols: [],
    name: '9Enum',
  };
  assertFailure(
    JSON.stringify(badEnumName),
    new Error("'9Enum' is not a valid avro name"),
    new Error("'9Enum' is not a valid avro name")
  );
});
test('rejects unrecognised custom types', () => {
  const schema: a.AvroType = {
    type: 'record',
    name: 'Person',
    fields: [{ name: 'age', type: 'intt' }],
  };
  assertFailure(
    JSON.stringify(schema),
    new Error(`intt is not a valid custom type name.`),
    new Error(`intt is not a valid custom type name.`)
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
  const expectedFlow = `
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
    export type __Schema = Union
  `;
  const expectedTs = `
    export type Person = {
      age: number,
    }
    export type Animal = {
      species: string
      }
    export type Union =
      |{
        Person: Person
        }
      | {
        Animal: Animal
        }
      | {
        string: string
        }
    export type __Schema = Union
  `;
  assertSuccess(schema, expectedFlow, expectedTs);
});

describe('union simplification', () => {
  test("doesn't create standalone types for simple nullable unions", () => {
    const schema = {
      type: 'record',
      name: 'Person',
      fields: [
        { name: 'spouse_name', type: ['null', 'string'] },
        {
          name: 'pet',
          type: [
            'null',
            {
              type: 'record',
              name: 'Pet',
              fields: [{ name: 'age', type: ['null', 'int'] }],
            },
          ],
        },
      ],
    };
    const expectedFlow = `
    // @flow

    export type Pet = {|
      age: null | {|
        int: number
      |},
    |}
    export type Person = {|
      spouse_name: null | {|
        string: string
        |},
      pet: null | {|
        Pet: Pet
        |}
    |}
    export type __Schema = Person
  `;
    const expectedTs = `
  export type Pet = {
    age: null | {
      int: number
    },
  }
  export type Person = {
    spouse_name: null | {
      string: string
      },
    pet: null | {
      Pet: Pet
      }
  }
  export type __Schema = Person
`;
    assertSuccess(schema, expectedFlow, expectedTs);
  });

  test('still creates standalone types for unions with more than 2 types', () => {
    const schema = {
      type: 'record',
      name: 'Person',
      fields: [{ name: 'age', type: ['null', 'int', 'double'] }],
    };
    const expectedFlow = `
    // @flow

    export type Person_Age =
      | null
      | {|
        int: number
        |}
      | {|
        double: number
        |}
    export type Person = {|
      age: Person_Age
    |}
    export type __Schema = Person
  `;
    const expectedTs = `
  export type Person_Age =
    | null
    | {
      int: number
      }
    | {
      double: number
      }
  export type Person = {
    age: Person_Age
  }
  export type __Schema = Person
`;
    assertSuccess(schema, expectedFlow, expectedTs);
  });

  describe('when wrapPrimitives = false', () => {
    test("doesn't wrap primitives in union branches", () => {
      const schema = {
        type: 'record',
        name: 'Person',
        fields: [{ name: 'age', type: ['null', 'int', 'double', 'string'] }],
      };
      const expectedFlow = `
      // @flow

      export type Person_Age =
        | null | number | string
      export type Person = {|
        age: Person_Age
      |}
    export type __Schema = Person
    `;

      const expectedTs = `
      export type Person_Age =
        | null | number | string
      export type Person = {
        age: Person_Age
      }
      export type __Schema = Person
    `;
      assertSuccess(schema, expectedFlow, expectedTs, false);
    });

    test('produces optional types for unions which have only null and a single primitive as a value', () => {
      const schema = {
        type: 'record',
        name: 'Person',
        fields: [{ name: 'age', type: ['null', 'int', 'double'] }],
      };
      const expectedFlow = `
      // @flow

      export type Person_Age = null | number
      export type Person = {|
        age: Person_Age
      |}
      export type __Schema = Person
    `;
      const expectedTs = `
      export type Person_Age = null | number
      export type Person = {
        age: Person_Age
      }
      export type __Schema = Person
    `;
      assertSuccess(schema, expectedFlow, expectedTs, false);
    });
  });
});
