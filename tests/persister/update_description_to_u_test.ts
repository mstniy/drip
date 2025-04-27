import { describe, it } from "bun:test";
import { strict as assert } from "assert";
import { updateDescriptionToU } from "../../src/persister/update_description_to_u";

describe("updateDescriptionToU", () => {
  it("works for an empty update description", () => {
    assert.deepStrictEqual(updateDescriptionToU({}), {});
  });
  it("works", () => {
    const res = updateDescriptionToU({
      disambiguatedPaths: {
        "a.b": ["a.b"],
        "a.c": ["a", "c"],
        "b.0": ["b", 0],
      },
      removedFields: ["a.b", "c"],
      truncatedArrays: [
        {
          field: "a.d",
          newSize: 5,
        },
        {
          field: "b.1",
          newSize: 3,
        },
      ],
      updatedFields: {
        d: 1,
        "b.0": "hey",
        "b.2": "there",
      },
    });

    assert.deepStrictEqual(res, {
      d: {
        "a.b": false,
        c: false,
      },
      sa: {
        t: {
          d: 5,
        },
      },
      sb: {
        i: {
          "0": "hey",
          "2": "there",
        },
        t: {
          "1": 3,
        },
      },
      i: {
        d: 1,
      },
    });
  });
});
