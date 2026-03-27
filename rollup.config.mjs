import typescript from '@rollup/plugin-typescript'

const input = 'guest-js/index.ts'

export default [
  {
    input,
    output: {
      dir: 'dist-js',
      entryFileNames: 'index.js',
      format: 'es',
      sourcemap: false,
    },
    plugins: [
      typescript({
        tsconfig: './tsconfig.rollup.json',
      }),
    ],
  },
  {
    input,
    output: {
      dir: 'dist-js',
      entryFileNames: 'index.cjs',
      exports: 'named',
      format: 'cjs',
      sourcemap: false,
    },
    plugins: [
      typescript({
        tsconfig: './tsconfig.rollup.json',
      }),
    ],
  },
]
