# iterlib

A library for easy iterator-based concurrency and parallelism.

| Build Status |
|--------------|
|[![CircleCI](https://circleci.com/gh/rohits2/iterlib.svg?style=svg)](https://circleci.com/gh/rohits2/iterlib)|

## What is this?

Have you ever been working with `map` or a generator and gotten annoyed with how slow lazy evaluation made some tasks?
Have you ever wondered "could I run this generator in the background?"

This library exists as the answer to that question.
It implements asynchronous preloading generators and parallel `map`.
Both the preloaders and the parallel `map` implementations support `multiprocessing` and `threading` as backends.

## How do I use it?
### Preload
Here's a simple example of preloading a generator:

```python3
from iterlib import thread_preload

gen = (x**2 for x in range(100000))
preloaded_gen = thread_preload(gen, buffer_size=100)
```

That's it! The generator will now preload up to 100 items in the background.  When you call `next(preloaded_gen)`, either directly or indirectly through a `for` statement, it will return values from the preloaded queue.

### Parallel Map
Preloading generators has a significant limitation: it's impossible to use more than one background executor because access to iterators requires synchronization.  However, most generators tend to be `map`s over other iterators, which opens an opportunity.  We can't parallelize reads from an iterator, but we can parallelize function calls.

Use one of `threaded_map` or `process_map` when you know your generator is a map:

```python3
from iterlib import thread_map

gen = [x for x in range(100000)]
mapped_gen = thread_map(lambda x: x**2, buffer_size=100, num_workers=4)
```

This will create an `ItemizedMap` named `mapped_gen`.  When you call `iter(mapped_gen)`, a generator will be created in the background that will preload up to 100 samples per worker (so 400 total in this example).

> **Careful**: These functions has different semantics than the regular Python `map`!  If you `map` over an indexable collection (like a list or numpy array) the returned `ItemizedMap` will also be an indexable collection that lazily evaluates the `map` for each element you access!  Only when `iter` is called (in a for loop or directly) will it return an asynchronous generator.

