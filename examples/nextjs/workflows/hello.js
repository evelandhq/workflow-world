import { sleep } from "workflow";

export async function hello(name) {
  "use workflow";
  const greeting = await greet(name);
  await sleep("2s");
  return greeting;
}

async function greet(name) {
  "use step";
  return `Hello, ${name}!`;
}
