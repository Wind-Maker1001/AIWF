function defineMethod(prototype, name, value) {
  Object.defineProperty(prototype, name, {
    value,
    configurable: true,
    writable: true,
  });
}

export { defineMethod };
