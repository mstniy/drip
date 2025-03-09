// @ts-check

import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  eslint.configs.recommended,
  tseslint.configs.recommended,
  tseslint.configs.recommendedTypeChecked,
  {
    rules: {
      "@typescript-eslint/no-floating-promises": [
        "warn",
        {
          allowForKnownSafeCalls: [
            {
              from: "package",
              name: ["it", "describe"],
              package: "node:test",
            },
          ],
        },
      ],
    },
  },
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: ".",
      },
    },
  },
  {
    ignores: ["coverage"],
  }
);
