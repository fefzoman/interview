# Python: Data Engineer Interview Guide

A Python ingestion job often starts as a short script: fetch records, clean them, and write the result. The interesting problems appear when the input grows, a request times out, or a retry repeats a write. This guide connects Python's language rules to those practical situations, then follows them through concurrency, testing, packaging, and production operation.

## Contents

1. [Language Fundamentals](#1-language-fundamentals)
2. [Internals & Memory Management](#2-internals--memory-management)
3. [Object-Oriented Programming](#3-object-oriented-programming)
4. [Advanced Python](#4-advanced-python)
5. [Concurrency & Parallelism](#5-concurrency--parallelism)
6. [Data Processing & Performance](#6-data-processing--performance)
7. [Software Design & Architecture](#7-software-design--architecture)
8. [Testing & Code Quality](#8-testing--code-quality)
9. [Packaging & Environments](#9-packaging--environments)
10. [Python for Data Engineering](#10-python-for-data-engineering)
11. [Production & Advanced Topics](#11-production--advanced-topics)

## 1. Language Fundamentals

### Data types, mutability, and hashability

A connector may receive an order ID as an integer in one payload and a string in another. Python lets a name refer to either object because it is dynamically typed: the type belongs to the object, and a name can be rebound. That flexibility does not make every operation valid. Trying `1 + "2"`, for example, still raises `TypeError`, so input handling needs an explicit type contract.

| Type | Mutable? | Typical use and interview detail |
| --- | --- | --- |
| `int` | No | Arbitrary-precision integers, limited by available memory; arithmetic cost grows with the number of digits |
| `float` | No | Binary floating point; many decimal fractions are approximate |
| `bool` | No | Boolean values; a subclass of `int`, so distinguish booleans when validating integer fields |
| `str`, `bytes` | No | Unicode text and raw bytes; decoding requires an explicit encoding contract |
| `list` | Yes | Ordered sequence; append is amortized O(1), searching is O(n) |
| `tuple` | No | Fixed sequence of references; referenced objects may still be mutable |
| `dict` | Yes | Insertion-ordered key/value mapping; keys must be hashable |
| `set` | Yes | Distinct hashable elements; no sequence ordering guarantee |
| `frozenset` | No | Immutable set; can be a mapping key |
| `NoneType` | No | `None` represents an absent value; distinguish absence from zero or an empty string |

**Mutable** means the object's state can change without replacing its identity. **Hashable** means it provides a hash that stays stable during its lifetime and equality compatible with that hash. Equal hashable objects must have equal hashes; equal hashes do not imply equal objects. A tuple containing a list is immutable as a container but unhashable. User-defined mutable objects can be hashable by identity, so mutability and hashability are related concepts, not synonyms. [Python data model](https://docs.python.org/3.14/reference/datamodel.html).

### Variables, references, identity, and equality

Assignment binds a name to an object; it does not copy that object. Function arguments follow the same model: the parameter becomes another local name referring to the supplied object. Mutating that object can affect the caller, while rebinding the parameter cannot rebind the caller's name.

```python
original = [1, 2]
alias = original
separate = [1, 2]

assert alias is original
assert separate == original
assert separate is not original

alias.append(3)
assert original == [1, 2, 3]

def replace_locally(items):
    items = []
    return items

assert replace_locally(original) == []
assert original == [1, 2, 3]
```

- `is` checks identity: whether both expressions refer to the same object. Use `value is None` for an absence check.
- `==` checks equality according to the type's comparison behavior. Libraries can return arrays of comparisons rather than one boolean.
- `id()` identifies an object during its lifetime. An identifier can be reused after the object is destroyed; it is not a persistent business key.

Do not use `is` to compare integer or string values. Interning and object reuse can make incorrect identity comparisons appear to work in a small example.

### Function arguments and mutable defaults

Default argument expressions run when the function definition executes, rather than on every call. A default list or dictionary can therefore accidentally retain state across records or pipeline runs.

```python
def add_tag(tag, tags=None):
    if tags is None:
        tags = []
    tags.append(tag)
    return tags

assert add_tag("new") == ["new"]
assert add_tag("paid") == ["paid"]
```

`*args` collects additional positional arguments; `**kwargs` collects additional keyword arguments. In a signature, `/` marks positional-only parameters, and parameters after `*` are keyword-only. A signature such as `load(rows, *, batch_size=1000)` makes the batch-size choice explicit at call sites. Avoid swallowing arbitrary keyword arguments when misspelled options should fail.

### Scope, LEGB, and closures

For ordinary nested functions, name lookup follows **LEGB**: local scope, enclosing function scopes, module globals, then builtins. Assignment normally makes a name local to that function unless declared `global` or `nonlocal`. Reading it before its local assignment raises `UnboundLocalError`, even if an outer name has the same spelling.

- `global` targets the module-level binding.
- `nonlocal` targets an existing binding in an enclosing function scope.
- A **closure** retains access to enclosing bindings after the outer function returns.

Closures capture bindings, so loop-created functions often all observe the loop variable's final value. Binding the current value as a default argument fixes that specific pattern:

```python
late_bound = [lambda: i for i in range(3)]
snapshots = [lambda i=i: i for i in range(3)]

assert [fn() for fn in late_bound] == [2, 2, 2]
assert [fn() for fn in snapshots] == [0, 1, 2]
```

Class bodies, comprehensions, and annotation scopes have additional rules; LEGB is a useful explanation for function scope, not a complete specification of every Python scope. Do not rely on a class body acting like an enclosing function for its methods. [Execution model](https://docs.python.org/3.14/reference/executionmodel.html).

### Decorators

A decorator receives a function or class and returns its replacement. `@decorate` above a function definition is approximately `function = decorate(function)`. Multiple decorators compose from the bottom upward: `@outer` above `@inner` means `outer(inner(function))`.

Common uses include instrumentation, registration, caching, and authorization. A wrapper should preserve return values and exceptions, and usually use `functools.wraps` to retain metadata. A synchronous wrapper is not automatically correct for an async function; it may time coroutine creation rather than execution. Section 4 shows a typed decorator.

### Iterables, iterators, generators, and comprehensions

| Concept | Contract | Practical consequence |
| --- | --- | --- |
| Iterable | `iter(value)` obtains an iterator | Lists, files, and many query results can supply a sequence of values |
| Iterator | `next(iterator)` supplies a value or raises `StopIteration`; `iter(iterator)` returns itself | Usually consumed once |
| Generator | A generator function call creates an iterator whose execution suspends at `yield` | State and local references survive between values |
| List/set/dict comprehension | Builds the requested collection eagerly | Concise, but retains all produced elements |
| Generator expression | Produces values lazily | Avoids retaining all output; does not automatically bound upstream state |

```python
records = [{"amount": 10}, {"amount": 20}]
amounts = (record["amount"] for record in records)

assert sum(amounts) == 30
assert list(amounts) == []
assert [record["amount"] * 2 for record in records] == [20, 40]
```

`range` is a compact, reusable sequence, not a generator. `zip` and `map` return iterators. `zip` silently stops at the shortest input unless `strict=True` is used; use the strict form when unequal lengths mean corrupted alignment. Prefer simple comprehensions to nested expressions that hide error handling or side effects.

### Context managers and exceptions

A context manager scopes resource ownership. `with` calls `__enter__`, executes the body, and then calls `__exit__` on normal completion or an exception. If entering fails, that manager's `__exit__` is not called. A truthy return from `__exit__` suppresses the body's exception, so suppression must be deliberate.

Use `with open(...)`, managed database transactions, and managed locks to make cleanup explicit. Releasing a resource must not depend on when garbage collection happens. `contextlib.ExitStack` helps when acquiring a dynamic number of resources. [Context manager utilities](https://docs.python.org/3.14/library/contextlib.html).

Exception handling separates failure paths:

- `try`: the operation that can fail.
- `except`: handles a specific failure.
- `else`: runs when the `try` body succeeds, avoiding an overly broad protected region.
- `finally`: cleanup on ordinary stack unwinding, including a return or exception. It cannot guarantee execution after a forced process kill or machine failure.

Catch the narrow exception you can handle. Use bare `raise` to propagate the current exception and `raise DomainError(...) from exc` to preserve the underlying cause. `except Exception` excludes exceptions such as `KeyboardInterrupt`, `SystemExit`, and `asyncio.CancelledError`. Avoid returning from `finally`, which can discard an earlier exception or result.

**Interview questions**

- **Why did changing a dictionary inside a function affect its caller?** Both names referred to the same dictionary; argument passing did not make a copy.
- **Is a tuple always safe as a dictionary key?** Only when its elements are hashable.
- **Why did the second pass over a file return nothing?** The iterator was exhausted; reopen or seek only when the source supports it.
- **Does a generator guarantee low memory?** No. Its producer, consumer, captured state, or grouping operation can still retain the entire dataset.

## 2. Internals & Memory Management

### CPython architecture, bytecode, and the interpreter

When a Python job consumes too much memory or fails to speed up with threads, the implementation matters. Python is the language; **CPython**, written primarily in C, is its widely used implementation. Other implementations can make different execution, memory, and concurrency choices. The following path describes how CPython takes source text to running work.

```text
Source text -> parsing / AST -> code objects containing bytecode
                                      |
                           interpreter executes frames
                                      |
                         Python objects and native extensions
```

CPython compiles Python source to bytecode and executes it through an interpreter. Therefore, “Python is interpreted” does not mean it has no compilation step. Imported modules may cache bytecode in `__pycache__`; a valid `.pyc` saves compilation work but does not make a slow algorithm fast.

Modern CPython specializes some operations at runtime. Bytecode instructions and optimization details vary across versions and are not a portable application interface. `dis` can explain execution, but counting bytecode instructions is not a substitute for measurement. Optional JIT facilities are build- and version-dependent; do not assume production Python automatically compiles every hot loop to machine code. [Bytecode inspection](https://docs.python.org/3.14/library/dis.html), [Python 3.14 implementation changes](https://docs.python.org/3.14/whatsnew/3.14.html).

### Object model, stack, and heap

Each object has a type, identity, and state. Python-level objects live in managed heap storage; containers usually hold references to other objects rather than embedding a uniform array of raw numeric values.

An execution frame tracks a function's code, local bindings, evaluation state, and references to other frames or objects. The call stack describes active calls; the native C stack and CPython's internal frame storage are implementation details. The Java-style claim “local values live on the stack, objects live on the heap” is too simple for Python: a local name can reference a large heap-allocated list, and a suspended generator can retain its frame after the calling function returns.

This object overhead explains why a list of Python integers usually needs much more memory than the equivalent fixed-width NumPy array. `sys.getsizeof(container)` reports the container's shallow size, not every object reachable through it.

### Allocation, reference counting, and cyclic garbage collection

CPython's memory manager uses allocators and reuse strategies to serve object allocations. Default and free-threaded builds can use different allocator paths; allocator details are not language guarantees. Releasing an object does not necessarily return its storage immediately to the operating system. [CPython memory management](https://docs.python.org/3.14/c-api/memory.html).

In ordinary GIL-enabled CPython, reference counting usually reclaims an object promptly when its reference count reaches zero. Some objects are immortal, and free-threaded builds change reference management details. Never make correctness depend on a particular destruction time.

Reference counting alone cannot reclaim a cycle whose members still reference each other. The cyclic garbage collector supplements it by finding unreachable cycles among tracked objects. Reachability matters: a global dictionary containing old records is still live, so collecting cycles cannot fix that retention. Collection behavior changes between versions and builds; tune it only after profiling. [Garbage collector interface](https://docs.python.org/3.14/library/gc.html).

`del name` removes a binding; it does not forcibly destroy a shared object. `gc.collect()` does not guarantee a lower resident set size (RSS). `__del__` is a poor primary mechanism for closing files, returning pooled connections, or committing transactions; use explicit ownership and context managers.

### Shallow copying and deep copying

| Operation | Copies outer container? | Copies nested objects? |
| --- | --- | --- |
| `alias = original` | No | No |
| `list(original)`, `original.copy()` for a list | Yes | No |
| `copy.copy(original)` | Typically, subject to the type's copy protocol | No recursive copy |
| `copy.deepcopy(original)` | Subject to the type's copy protocol | Recursively copies supported contents, tracking already-seen objects |

```python
from copy import deepcopy

source = {"tags": ["raw"]}
shallow = source.copy()
deep = deepcopy(source)

source["tags"].append("checked")
assert shallow["tags"] == ["raw", "checked"]
assert deep["tags"] == ["raw"]
```

Deep copy handles many cycles through a memo of objects already copied, but is not a universal cloning operation. Connections, locks, generators, native buffers, and external resource ownership require type-specific handling. Copying an entire batch to avoid mutating two fields can waste memory; a new output record with explicitly selected fields is often clearer. [Copy protocol](https://docs.python.org/3.14/library/copy.html).

### GIL and thread safety

In the usual GIL-enabled CPython build, a thread must hold the **global interpreter lock (GIL)** to execute Python code. Blocking I/O commonly releases it, and native libraries can release it around eligible computation. Consequently, threads can overlap network waiting and some native numeric work, while ordinary Python CPU loops generally cannot execute in parallel within one interpreter.

The GIL protects interpreter internals; it does not make a business operation atomic. A sequence that reads a balance, checks a limit, and writes a new balance still requires synchronization. Do not infer thread safety from an operation appearing to be one line of Python. [CPython thread state and GIL](https://docs.python.org/3.14/c-api/init.html#thread-state-and-the-global-interpreter-lock).

### Free-threaded Python

Python 3.13 introduced an optional free-threaded CPython build; Python 3.14 makes that build officially supported while keeping it optional. Threads can execute Python code across cores when the GIL is disabled. That changes a concurrency option, rather than removing the need to design ownership and synchronization. [Python 3.14 free-threading status](https://docs.python.org/3.14/whatsnew/3.14.html#free-threaded-python-is-officially-supported).

Check both the build and running process. A compatible build can still run with the GIL enabled, and importing an extension without declared support can enable it. `sysconfig.get_config_var("Py_GIL_DISABLED")` identifies build support; `sys._is_gil_enabled()` reports the running state where available. Built-in containers use internal protection, but application invariants still need explicit locks. Dependency compatibility, memory, and scaling require measurement. [Free-threading guide](https://docs.python.org/3.14/howto/free-threading-python.html).

**Interview questions**

- **Why does RSS stay high after `del records`?** Other references, allocator retention, fragmentation, or native buffers may keep memory resident. Identify which before declaring a leak.
- **Does Python collect circular references?** CPython has a cyclic collector in addition to reference counting; cycles reachable from live roots are not garbage.
- **Does removing the GIL make code thread-safe?** No. Shared application state still needs an ownership and synchronization strategy.
- **Why can NumPy threads sometimes scale despite the GIL?** Eligible native kernels release it; Python object operations and specific libraries can behave differently.

## 3. Object-Oriented Programming

### Classes, inheritance, polymorphism, and encapsulation

Suppose each ingestion run needs its own list of rejected records. A class can keep that state alongside the operations that manage it, but the attribute's location matters: instance attributes belong to individual objects, while class attributes are shared through the class unless shadowed. Putting a mutable list on the class and treating it as private run state can make records leak between runs.

- **Inheritance:** a subtype reuses or refines a parent interface and implementation.
- **Polymorphism:** callers use a common operation with multiple concrete implementations, such as `sink.write(rows)`.
- **Encapsulation:** expose operations that preserve invariants rather than allowing callers to manipulate every internal detail.

Python commonly uses duck typing: an object is acceptable when it supports the required behavior. `_name` marks a nonpublic convention; `__name` triggers name mangling to avoid some subclass collisions. Neither is a security boundary. `__new__` creates an instance; `__init__` initializes an instance after creation and must return `None`.

Use an ordinary function for a stateless transformation. A class is useful when there is meaningful state or a lifecycle, such as a managed connection pool; wrapping every function in a class adds little.

### MRO, multiple inheritance, and `super()`

The **method resolution order (MRO)** determines attribute lookup through a class hierarchy. Python uses C3 linearization, preserving local parent order and monotonicity; inconsistent hierarchies are rejected when the class is created. Inspect `SomeClass.__mro__` rather than guessing a depth-first traversal.

`super()` delegates to the next implementation in the current object's MRO. It does not mean “call my directly named parent,” which is why cooperative multiple inheritance can avoid calling a common ancestor twice.

```python
class Root:
    def path(self):
        return ["Root"]

class Left(Root):
    def path(self):
        return ["Left"] + super().path()

class Right(Root):
    def path(self):
        return ["Right"] + super().path()

class Combined(Left, Right):
    def path(self):
        return ["Combined"] + super().path()

assert Combined().path() == ["Combined", "Left", "Right", "Root"]
```

Cooperative methods need compatible calling conventions and a consistent policy for consuming and forwarding arguments. Mixing direct parent calls with cooperative calls can skip or duplicate work. Prefer small mixins with clear behavior over an elaborate diamond for pipeline configuration. [Python MRO explanation](https://docs.python.org/3.14/howto/mro.html).

### Dunder methods and protocols

Special methods connect objects to language operations. Implement the relevant protocol rather than inventing a parallel method name.

| Protocol or operation | Methods | Example use |
| --- | --- | --- |
| Representation | `__repr__`, `__str__` | Debugging and readable output; exclude secrets |
| Equality and hashing | `__eq__`, `__hash__` | Record keys and deduplication |
| Iteration | `__iter__`, `__next__` | A streaming record reader |
| Containers | `__len__`, `__getitem__`, `__contains__` | Sequence or mapping-like access |
| Context management | `__enter__`, `__exit__` | Resource or transaction ownership |
| Async context/iteration | `__aenter__`, `__aexit__`, `__aiter__`, `__anext__` | Async clients and streams |
| Callable objects | `__call__` | Stateful validation or transformation |

When a comparison does not support the other operand's type, return `NotImplemented` so Python can try the other operand's implementation. That is different from raising `NotImplementedError`. If a class defines value equality but lacks a suitable hash, its instances should not be mapping keys. A mutable value-based hash breaks lookup after mutation. [Special method names](https://docs.python.org/3.14/reference/datamodel.html#special-method-names).

### Descriptors and properties

A **descriptor** defines `__get__`, `__set__`, or `__delete__` and customizes attribute access when stored on a class. Functions are descriptors: looking up an instance method binds the instance as its first argument. Properties are descriptors too.

For ordinary attribute lookup, a data descriptor has priority over an instance dictionary; a non-data descriptor can be shadowed by an instance attribute. A `property` with only a getter is still a data descriptor and rejects assignment through the property.

Use `@property` for cheap derived state or validated access where an attribute-shaped interface is appropriate. A property that unexpectedly downloads a large object or performs a database scan hides cost from callers. Descriptors are useful for reusable attribute mechanics, but are unnecessary for a one-off validation that a constructor can express. [Descriptor guide](https://docs.python.org/3.14/howto/descriptor.html).

### Dataclasses and abstract base classes

`dataclasses.dataclass` generates common methods such as initialization, representation, and equality from annotated fields. It does not validate field types. `field(default_factory=list)` makes an independent default collection per instance; `frozen=True` prevents ordinary field rebinding but does not freeze nested mutable objects.

```python
from dataclasses import dataclass, field

@dataclass
class Batch:
    source: str
    rows: list[dict] = field(default_factory=list)

first = Batch("orders")
second = Batch("payments")
first.rows.append({"id": 1})
assert second.rows == []
```

`slots=True` can reduce per-instance overhead and restrict ordinary dynamic attributes, but is not a universal memory fix; measure object volume and inheritance constraints. Dataclass equality normally requires the same concrete class. [Dataclasses](https://docs.python.org/3.14/library/dataclasses.html).

An **abstract base class (ABC)** uses `abc.ABC` and `@abstractmethod` to prevent instantiation until required abstract methods are implemented. It is useful when implementations deliberately share a nominal base and lifecycle. A `typing.Protocol` describes structural compatibility without requiring inheritance; section 4 compares them.

### Composition, inheritance, and SOLID

Composition means an object receives or owns another object and delegates work to it. An ingestion job that accepts a source and sink can change either independently. Inheriting `ProductionOrdersJobFromRetryingJsonHttpJob` couples unrelated choices into a fragile hierarchy.

| Principle | Meaning | Data engineering example |
| --- | --- | --- |
| Single responsibility | Keep a unit focused on one reason to change | Parsing business fields should not also manage deployment credentials |
| Open/closed | Extend behavior through stable boundaries when variation is real | Add a sink implementing the existing write contract |
| Liskov substitution | A subtype must honor its parent's behavioral promises | A streaming reader subtype should not silently return only the first page |
| Interface segregation | Expose the smallest useful interface | A record reader need not implement write/delete methods |
| Dependency inversion | Business rules depend on capabilities, not infrastructure details | Pass a writer into a transformation workflow |

These are design questions, not a requirement to add five interfaces. Choose inheritance for a genuine substitutable subtype or a framework extension point; choose composition for independent behavior and replaceable dependencies.

**Interview questions**

- **Why use `super()` in a mixin?** It allows the full MRO to cooperate instead of hard-coding a parent and potentially skipping another mixin.
- **Does a frozen dataclass guarantee deeply immutable data?** No. A field can still refer to a mutable list or dictionary.
- **When is a protocol preferable to an ABC?** When callers only need a small behavior and implementations should not have to share a base class.
- **Should an ETL job inherit from its database client?** Usually no: the job uses a client; it is not a substitute for one.

## 4. Advanced Python

### Closures, higher-order functions, and typed decorators

Several connectors may need the same retry or timing behavior with different settings. A **higher-order function** can express that reuse by accepting a callable, returning one, or both. A closure can retain configuration without a class, and `functools.partial` can bind selected arguments to an existing function. Captured mutable state also survives between calls, so make that persistence deliberate.

The following synchronous timing decorator preserves the wrapped callable's parameter types with `ParamSpec` and its result type with `TypeVar`. The timer logs on success and on failure without swallowing the original exception.

```python
import logging
from functools import wraps
from time import perf_counter
from typing import Callable, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")
logger = logging.getLogger(__name__)

def timed(function: Callable[P, R]) -> Callable[P, R]:
    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start = perf_counter()
        try:
            return function(*args, **kwargs)
        finally:
            logger.info("function=%s duration_s=%.6f",
                        function.__name__, perf_counter() - start)
    return wrapper

@timed
def total(values: list[int]) -> int:
    return sum(values)

assert total([1, 2, 3]) == 6
assert total.__name__ == "total"
```

Decorators execute when the definition is evaluated, often at module import. Heavy registration work can therefore slow worker or DAG startup. Order matters: caching a timed function measures cache misses inside the timer, while timing a cached function includes cache lookups. `functools.cache` is unbounded; prefer bounded caching where input cardinality can grow.

### `yield`, generator expressions, and `yield from`

A function containing `yield` normally returns a generator when called; its body executes as the generator is advanced. At each suspension, local state is retained. `return value` ends it and exposes that value through `StopIteration.value`, rather than yielding another record.

`yield from iterable` delegates iteration. For a subgenerator, delegation also forwards generator protocol operations and makes its return value available to the delegating generator. It is more than a different spelling for concatenating lists.

```python
def validated_ids(values):
    for value in values:
        if type(value) is not int or value < 0:
            raise ValueError("IDs must be nonnegative integers")
        yield value

def all_ids(groups):
    for group in groups:
        yield from validated_ids(group)

assert list(all_ids([[1, 2], [], [3]])) == [1, 2, 3]
```

Errors in a generator often appear during consumption rather than construction, so wrap the consuming operation when handling them. A generator over an open file needs explicit ownership if iteration can stop early. Do not return an iterator from inside a `with open(...)` block after closing the file it needs. Async generators use `async for`; they do not support `yield from`.

### Type hints, generics, Protocol, and TypedDict

Type hints describe intended contracts for tools and readers; they do not validate untrusted runtime data. Use explicit parsing and validation at external boundaries. `Any` largely disables static checking for operations involving that value; `object` allows any object but requires narrowing before type-specific use. `cast()` changes a type checker's view, not the runtime object. [Typing reference](https://docs.python.org/3.14/library/typing.html).

| Tool | Purpose | Limitation |
| --- | --- | --- |
| `list[int]`, `Mapping[str, int]` | Describe container element types | Existing data is not checked at runtime |
| `T`, `TypeVar`, generics | Preserve relationships between input and output types | A vague unconstrained generic can hide a missing concrete contract |
| `Protocol` | Structural interface: an object provides the specified members | Static compatibility does not prove semantic correctness |
| `TypedDict` | Describe keys and value types in a dictionary | Runtime value is still an ordinary dictionary |
| `dataclass` | Concrete record object with generated methods | Annotations do not enforce validation |
| ABC | Shared nominal interface with abstract methods | Implementations normally need the intended inheritance relationship |

```python
from collections.abc import Sequence
from typing import NotRequired, Protocol, TypedDict

class Order(TypedDict):
    order_id: int
    amount_cents: int
    note: NotRequired[str]

class OrderSink(Protocol):
    def write(self, rows: Sequence[Order]) -> None: ...

class MemorySink:
    def __init__(self):
        self.rows: list[Order] = []

    def write(self, rows: Sequence[Order]) -> None:
        self.rows.extend(rows)

def publish(rows: Sequence[Order], sink: OrderSink) -> None:
    sink.write(rows)

sink = MemorySink()
publish([{"order_id": 1, "amount_cents": 250}], sink)
assert sink.rows == [{"order_id": 1, "amount_cents": 250}]
```

`NotRequired[str]` means a key may be absent; `str | None` means a present value may be `None`. `@runtime_checkable` allows limited runtime protocol checks, but does not validate full signatures or behavior.

Python 3.12+ supports syntax such as `def first[T](items: Sequence[T]) -> T: ...` and `type Row = dict[str, object]`. Older supported versions can use `TypeVar` and assignment-based aliases. Mutable containers such as `list[T]` are invariant: treating a `list[int]` as a `list[object]` would allow a caller to insert a string. A read-only interface such as `Sequence` often expresses a broader valid input contract. [Python typing concepts](https://typing.python.org/en/latest/reference/generics.html).

### Metaclasses, introspection, and reflection

Classes are objects too. A **metaclass**, usually `type`, controls class creation. It can inspect a class namespace or customize registration. Most business code does not need a custom metaclass: a class decorator, `__init_subclass__`, or an explicit dictionary registry often solves the real requirement with less hidden behavior.

**Introspection** examines objects, for example with `type`, `isinstance`, `dir`, `vars`, and `inspect.signature`. **Reflection** also changes or invokes behavior dynamically, using tools such as `getattr`, `setattr`, and `importlib.import_module`.

Dynamic access can execute properties or other user code. Validate externally supplied operation names against an allowlist rather than importing arbitrary dotted paths. Avoid `eval` for configuration. In Python 3.14, annotations are deferred by default, and resolving annotations or type hints can execute expressions; do not evaluate annotations from untrusted code as if they were passive schema metadata. [Annotation behavior](https://docs.python.org/3.14/whatsnew/3.14.html#deferred-evaluation-of-annotations).

### Functional programming and immutability

A pure transformation returns a result from its inputs without observable side effects. Separating pure normalization from network calls and writes makes tests deterministic and replay safer. Prefer comprehensions and named functions when they are clearer than nested `map`, `filter`, and `lambda` expressions; `sorted(..., key=...)` is a useful higher-order API.

Immutability reduces accidental shared-state changes, but a tuple or frozen dataclass can contain mutable values. Replacing every intermediate record can also increase allocations. Aim for clear ownership: transform into a new output record where necessary, and avoid copying data that is already exclusively owned. `itertools` and `functools` offer composition and iteration utilities without adding a functional programming dependency.

**Interview questions**

- **What does `functools.wraps` preserve?** Useful metadata and a `__wrapped__` reference for introspection; it does not automatically preserve a wrapper's static signature without suitable typing.
- **Does `TypedDict` reject a bad API payload?** No. Validate runtime structure and values before treating a payload as that type.
- **Why can a generator keep a large object alive?** Its suspended frame can still reference that object.
- **When would you use a metaclass?** For unavoidable class-creation behavior across a framework; explain why a simpler class hook or decorator is insufficient.

## 5. Concurrency & Parallelism

### Threading, multiprocessing, and asyncio

While one API request waits for the network, a job may be able to start another. That is **concurrency**: operations make progress over overlapping periods. **Parallelism** goes further and executes work simultaneously. An event loop can manage many network requests on one thread, but it does not thereby run their Python code in parallel. The right model depends on where the job spends its time.

| Model | Useful workload | Shared state | Main cost or failure mode |
| --- | --- | --- | --- |
| Threads | Blocking I/O; native kernels that release the GIL | One process address space | Races, locks, blocked workers, connection limits |
| Processes | Substantial Python CPU work; isolation | Separate address spaces by default | Startup, serialization, duplicated memory, worker failure |
| `asyncio` | Many I/O operations with async-capable libraries | Tasks usually share one event-loop thread | Blocking the loop, unbounded tasks, cancellation mistakes |
| Free-threaded threads | Python CPU work with compatible dependencies | One address space | Shared-state races, compatibility, synchronization overhead |

Start with the bottleneck. For a pipeline waiting on an API, adding processes may add overhead without reducing waiting. For CPU-heavy parsing in Python, asyncio cannot create more CPU execution capacity. For a native engine already using all cores, another process or thread pool can oversubscribe the machine.

### Event loops, coroutines, tasks, and futures

An event loop schedules callbacks and ready tasks and reacts to I/O readiness. Calling an `async def` function creates a coroutine object; executing it requires awaiting it or scheduling it as a task. `await first(); await second()` is sequential unless the operations have already been scheduled concurrently.

A **task** schedules and tracks a coroutine. A **future** represents an eventual result or exception. `asyncio.Future` integrates with the event loop, while `concurrent.futures.Future` belongs to executor APIs; use the appropriate bridge rather than interchanging them.

`await` is a possible suspension point, not a guarantee that every await expression yields to another task: an already available result may complete immediately. A long CPU loop, `time.sleep`, or synchronous database request inside an async function can block every task on that loop. [Async event loop](https://docs.python.org/3.14/library/asyncio-eventloop.html), [Async futures](https://docs.python.org/3.14/library/asyncio-future.html).

### Bounded async work and task groups

`asyncio.TaskGroup` scopes child-task lifetime. On a child failure other than cancellation, it normally cancels remaining children, waits for them, and raises grouped failures. `asyncio.gather` has different failure semantics: its default propagation of one exception does not automatically cancel all the other submitted operations. [Coroutines and task groups](https://docs.python.org/3.14/library/asyncio-task.html).

This self-contained example simulates I/O. It limits active operations with a semaphore and limits submitted tasks by processing one small batch at a time; `itertools.batched` requires Python 3.12+.

```python
import asyncio
from itertools import batched

async def fetch_one(item, semaphore):
    async with semaphore:
        await asyncio.sleep(0)
        return item * 2

async def main():
    semaphore = asyncio.Semaphore(5)
    count = 0
    for batch in batched(range(100), 20):
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(fetch_one(item, semaphore))
                     for item in batch]
        results = [task.result() for task in tasks]
        assert results == [item * 2 for item in batch]
        count += len(results)
    assert count == 100

asyncio.run(main())
```

A semaphore alone limits active operations, not the number of waiting tasks. Creating a million tasks and making them all wait on a semaphore can exhaust memory. For continuous ingestion, fixed workers consuming a bounded queue can keep utilization steadier than batches. Bound response sizes and result retention too.

### Locks, semaphores, and queues

| Primitive | Protects or limits | Important detail |
| --- | --- | --- |
| `threading.Lock` | A critical section shared by threads | Protect the entire invariant, including read/check/write |
| `threading.RLock` | A reentrant critical section | Useful when the same thread must acquire the same lock recursively |
| `asyncio.Lock` | Task access to shared state on an event loop | Not a thread/process synchronization mechanism |
| Semaphore | Number of simultaneous operations | Does not enforce requests per second |
| `queue.Queue(maxsize=...)` | Thread producer/consumer handoff | A full queue blocks producers and creates backpressure |
| `asyncio.Queue(maxsize=...)` | Async producer/consumer handoff | Use `await`; not safe as a direct cross-thread queue |
| `multiprocessing.Queue` | Cross-process message passing | Usually serializes values; it is not zero-copy shared memory |

**Backpressure** means slower consumers constrain producers instead of allowing unlimited buffering. A bounded queue helps expose a downstream bottleneck before it exhausts memory. `task_done()` and `join()` track completed queue items; they do not stop workers automatically. Avoid holding a thread lock across `await`, and use a consistent lock order when several locks are necessary. [Thread synchronization](https://docs.python.org/3.14/library/threading.html), [Async queues](https://docs.python.org/3.14/library/asyncio-queue.html).

### Thread pools and process pools

Executors reuse workers. `submit` returns a future; calling `result()` surfaces the worker's result or exception. `map` normally yields results in input order, which can hold later completed results behind a slow early item. `as_completed` yields futures as they finish; preserve any required business ordering explicitly.

For process pools, the callable, arguments, and returned values normally need to be picklable. Define workers at module level, protect process creation with `if __name__ == "__main__":`, and create process-local clients rather than sending a live database connection to a worker.

Save this block as a script and run it with Python 3.14+: `buffersize` bounds pending map work, while `chunksize` groups inputs for process dispatch.

```python
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

def sum_squares(limit):
    return sum(value * value for value in range(limit))

if __name__ == "__main__":
    limits = [10, 20, 30, 40]
    with ProcessPoolExecutor(
        max_workers=2, mp_context=get_context("spawn")
    ) as pool:
        actual = list(pool.map(sum_squares, limits, chunksize=2, buffersize=4))
    assert actual == [sum_squares(limit) for limit in limits]
```

The tiny inputs demonstrate mechanics; they are too small to justify process overhead. Do not assume `fork` is the default: Python 3.14 moved eligible POSIX defaults to `forkserver`; macOS and Windows use `spawn`. Selecting `spawn` explicitly makes this example independent of those defaults. [Process start methods](https://docs.python.org/3.14/library/multiprocessing.html#contexts-and-start-methods).

### Cancellation, timeouts, and shutdown

Async cancellation is cooperative: `CancelledError` is normally delivered at a suspension point. Clean up in `finally` and propagate cancellation unless a deliberate higher-level policy handles it. `asyncio.timeout` scopes a timeout; it cannot preempt arbitrary synchronous code blocking the event loop. [Task cancellation and timeouts](https://docs.python.org/3.14/library/asyncio-task.html#task-cancellation).

For executors, cancelling a future normally works only before its work starts. A timeout while waiting for a result does not stop the running function. Cancelling an await around `asyncio.to_thread` likewise does not forcibly stop that thread. Use operation-level I/O timeouts and cooperative stop signals. Forceful process termination can leave partial external side effects and requires reconciliation. [Executor lifecycle and cancellation](https://docs.python.org/3.14/library/concurrent.futures.html).

Graceful shutdown stops admission, signals workers, allows a bounded drain, releases resources, and records what remains incomplete. A nested submission that waits on the same saturated pool can deadlock; do not make every worker wait for work that only those workers can execute.

**Interview questions**

- **How would you download 10,000 objects?** Use a reusable client, bounded I/O concurrency, connection limits, timeouts, rate control, and incremental consumption of results.
- **Why is a semaphore insufficient for a rate limit?** Ten concurrent requests can generate many more than ten requests per second when responses are fast.
- **Why are eight processes slower than one?** Work may be too small, serialization too expensive, memory bandwidth saturated, or native pools oversubscribed.
- **Does timing out a write mean it failed?** No. The remote system may have committed; recovery needs an idempotency key or a status check.

## 6. Data Processing & Performance

### Iterative versus vectorized processing

A loop that cleans ten records may be perfectly adequate; the same loop over ten million records can spend much of its time in Python-level iteration and object access. A vectorized expression delegates array or column operations to compiled kernels, often reducing that per-element overhead. It still performs work proportional to the relevant data and may allocate large intermediate arrays, so vectorization does not remove the need to consider memory.

For an installed NumPy version, this example performs array operations and checks its output:

```python
import numpy as np

prices = np.array([100, 200, 300], dtype=np.int64)
quantities = np.array([2, 1, 4], dtype=np.int64)
line_totals = prices * quantities
assert line_totals.tolist() == [200, 200, 1200]
assert int(line_totals.sum()) == 1600
```

Fixed-width numeric arrays can overflow where Python integers would grow. Broadcasting can avoid physically copying a small input, but the output and intermediates can still be huge. `np.vectorize` is a convenience wrapper around Python calls, not a promise of compiled vectorized speed. [NumPy broadcasting](https://numpy.org/doc/stable/user/basics.broadcasting.html), [NumPy vectorize](https://numpy.org/doc/stable/reference/generated/numpy.vectorize.html).

Reduce data movement first: select fewer columns, filter early when semantics allow, and push aggregation into a capable database or query engine. Replacing a loop with a library call helps only if that call avoids the same Python-level work.

### Pandas, Polars, PyArrow, and NumPy

| Tool | Main role | Useful operations | Limits to discuss |
| --- | --- | --- | --- |
| NumPy | Typed n-dimensional arrays and numeric kernels | Numeric transforms, broadcasting, linear algebra | Fixed-width types, views/copies, large intermediates |
| pandas | Labeled tables with a broad analysis ecosystem | Joins, grouping, time series, tabular I/O | Typically in-memory; Python object columns and copies can be expensive |
| Polars | Columnar DataFrames with an expression engine | Parallel execution, lazy queries, supported streaming plans | Python UDFs can limit optimization; not every plan has bounded memory |
| PyArrow | Arrow columnar data, compute, IPC, and file/dataset interfaces | Parquet batches, schema-aware interchange | Conversions can copy; native buffers also consume memory |

Arrow is an in-memory columnar representation; Parquet is an on-disk format. PyArrow implements APIs for both. Zero-copy interchange is possible only when layouts, types, lifetimes, and the consuming interface support it.

For pandas, prefer column expressions over per-row `apply` when they express the operation correctly. Choose dtypes deliberately, project needed columns, and use chunked readers for suitable formats. Repeatedly concatenating into a growing DataFrame creates avoidable copying. Copy-on-Write and string storage behavior depend on the pandas version; do not assume `copy=False` is a universal zero-copy guarantee. [Scaling pandas](https://pandas.pydata.org/docs/user_guide/scale.html), [Pandas Copy-on-Write](https://pandas.pydata.org/docs/user_guide/copy_on_write.html).

Polars' lazy API builds a plan before execution, enabling projection and predicate pushdown where supported. The following requires Polars and an `orders.parquet` file with the named columns:

```python
import polars as pl

query = (
    pl.scan_parquet("orders.parquet")
    .filter(pl.col("status") == "paid")
    .group_by("customer_id")
    .agg(pl.col("amount_cents").sum().alias("total_cents"))
)
print(query.explain())
totals = query.collect()
```

Calling `read_parquet` eagerly and converting the result to lazy form cannot undo the initial full read. Examine the plan and installed engine's streaming support; a lazy query alone does not prove a global group-by fits in memory. [Polars lazy optimizations](https://docs.pola.rs/user-guide/lazy/optimizations/).

### Streaming, generators, and chunking

Memory-efficient processing needs bounded state across the entire path: reader, parser, transformation, concurrency queue, writer, and retained results. A generator cannot compensate for a consumer calling `list()` on every row.

```python
from itertools import islice

def chunks(iterable, size):
    if size <= 0:
        raise ValueError("size must be positive")
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch

assert list(chunks(range(5), 2)) == [[0, 1], [2, 3], [4]]
assert list(chunks([], 2)) == []
```

This Python 3.11-compatible helper retains at most one batch itself. Python 3.12+ offers `itertools.batched`, returning tuples. A row-count limit is not a byte limit: one oversized payload can dominate a batch. Measure decoded memory, not just compressed input size.

Chunking naturally supports independent transforms and associative partial aggregates. Exact distinct counting still needs state proportional to distinct keys unless it spills, partitions, or uses an approximation. Global sorting and large joins need an explicit plan for state, not merely a chunked reader.

### Serialization formats

| Format | Strength | Weakness or safety concern | Typical use |
| --- | --- | --- | --- |
| JSON / JSON Lines | Portable, readable, widely supported | Verbose; dates, decimals, bytes, and schemas need conventions; giant documents are not automatically streamed | APIs, event payloads, line-oriented landing data |
| Pickle | Represents many Python-specific objects and graphs | Loading can execute arbitrary code; compatibility and imports matter | Trusted, controlled Python-only transfer |
| Parquet | Columnar, compressed, typed, supports selective reads | Encoding/writer support varies; excessive tiny files and row groups hurt scans | Analytical storage and batch interchange |
| Avro | Schema-based record serialization with schema resolution rules | Requires writer/reader schema discipline; not columnar | Events, records, and cross-language exchange |

Never unpickle untrusted content. Checksums verify integrity, not whether a producer is trusted. JSON avoids pickle's object reconstruction execution model, but still needs input-size limits, structural validation, and a contract for numeric precision. Standard JSON does not include `NaN` and infinity, despite permissive behavior in some encoders. [Pickle security](https://docs.python.org/3.14/library/pickle.html), [JSON behavior](https://docs.python.org/3.14/library/json.html).

An Avro object container file includes its writer schema; a bare binary record needs the writer schema through another agreement, often a schema registry. Adding a field is not automatically compatible in both directions: resolution depends on reader/writer schemas and defaults. [Avro 1.12 specification](https://avro.apache.org/docs/1.12.0/specification/).

For Parquet, row-group size trades scan parallelism, compression, metadata, and memory. A writer should close successfully before publication; a half-written file may lack required footer metadata. Logical types, decimal precision, timezone interpretation, and nullability need cross-engine tests. [Parquet format](https://parquet.apache.org/docs/file-format/).

### Profiling CPU, memory, and elapsed time

| Tool or measurement | Answers | What it misses |
| --- | --- | --- |
| `time.perf_counter()` | How long did an operation take in elapsed time? | Why it took that time |
| `time.process_time()` | How much CPU time did this process consume? | Waiting time and CPU used by separate worker processes |
| `cProfile` / `pstats` | Which calls account for time and call counts? | Fine detail inside many native kernels; profiling adds overhead |
| `tracemalloc` | Where did traced Python allocations originate and grow? | Not all native/driver allocations or total RSS |
| RSS/container memory and native allocation tools | What is the actual process/container footprint? | Which Python reference retains memory without further analysis |

For an actual script named `pipeline.py`, run `python -m cProfile -s cumulative pipeline.py`. Cumulative time includes callees; internal time excludes their execution. A function can dominate cumulative time because it waits for a database, so pair call profiles with latency and I/O measurements. [Python profilers](https://docs.python.org/3.14/library/profile.html).

```python
import tracemalloc

tracemalloc.start()
before = tracemalloc.take_snapshot()
records = [{"id": value} for value in range(10_000)]
after = tracemalloc.take_snapshot()
for allocation in after.compare_to(before, "lineno")[:3]:
    print(allocation)
assert len(records) == 10_000
tracemalloc.stop()
```

Compare snapshots around representative repeated work to find growth. Tracing should start before the allocations of interest. Python-tracked allocations can look stable while a native library's buffers or a growing queue increases RSS. [Tracemalloc](https://docs.python.org/3.14/library/tracemalloc.html).

### Complexity and an optimization order

| Operation | Typical complexity | Caveat |
| --- | --- | --- |
| List append | Amortized O(1) | Occasional resize copies references |
| List membership | O(n) | Repeated membership inside a loop can become O(n²) |
| Dictionary/set lookup | Average O(1) | Worst cases and expensive hashing/equality change cost |
| Sorting n elements | O(n log n) worst-case comparisons | Already ordered input can be faster; key/comparison cost matters |
| Deque append/pop at either end | O(1) | Middle access is not list-like random access |
| Hash aggregation | Expected O(n) processing | State usually grows with the number of distinct groups |

Measure a baseline, identify CPU/memory/I/O limits, fix algorithmic waste and repeated work, reduce data read and transferred, batch expensive boundaries, and then tune execution or parallelism. Record both speed and peak memory. A change that halves CPU time but causes out-of-memory retries can make the pipeline slower overall.

**Interview questions**

- **Why can `DataFrame.apply(axis=1)` be slow?** It often invokes Python per row, losing the benefit of native column operations.
- **Can a 10 GB file fit in 16 GB RAM?** File size alone is insufficient: compression, decoded types, copies, join expansion, and concurrent buffers determine peak memory.
- **How do you deduplicate a stream in constant memory?** Exact unbounded deduplication over an unbounded key domain is not generally possible; specify a bounded window, external state, or approximation.
- **Why is the first benchmark run slower?** Imports, caches, connection setup, specialization, and data warming can affect it; measure the workload's real cold/warm behavior.

## 7. Software Design & Architecture

### Modular architecture and separation of concerns

Consider a helper that converts a timestamp and also advances the ingestion checkpoint. If conversion succeeds but publication fails later, the next run could skip data. A clearer ingestion boundary separates external acquisition, deterministic normalization, validation, and durable publication. Keep source-specific response parsing outside business transformations, and let the part that knows publication succeeded own checkpoint advancement.

```text
Source adapter -> parse / validate -> transform -> sink / transaction
       |                                               |
       +---------- orchestration and checkpoints -------+
                         |
                 logs, metrics, tracing
```

These are responsibilities, not a requirement for a folder or class per arrow. Start with functions in a small module and split when separate ownership, dependencies, or testing needs justify it. Give modules explicit inputs and return values; avoid import-time network requests and hidden global clients that make tests and worker startup unpredictable.

### Design patterns and dependency injection

Patterns name existing solutions; they are useful when they describe an actual requirement.

| Pattern | When it helps | Small Python implementation |
| --- | --- | --- |
| Strategy | Choose one transformation or writing behavior | Pass a function or protocol-compatible object |
| Adapter | Normalize a vendor API to an internal contract | A small source-specific function or object |
| Factory | Construction needs environment-dependent configuration | An ordinary function returning the configured client |
| Context manager | Pair resource acquisition and release | `with`, `contextlib`, or an existing client context |
| Dependency injection | Substitute I/O or policies without changing business rules | Pass the client, writer, clock, or sleep function explicitly |

The `publish(rows, sink)` example in section 4 already demonstrates dependency injection; no container framework is required. Inject at meaningful boundaries rather than exposing every implementation detail as a configuration option. A module-level singleton can complicate forks, tests, credentials, and cleanup; prefer an explicit application lifecycle when the dependency has state.

### Configuration-driven applications

Keep deployment values outside business logic: source endpoints, destination identifiers, batch size, timeouts, and concurrency. Separate secrets from ordinary config and resolve secrets at runtime through the deployment's approved mechanism.

Define precedence, for example command-line arguments over environment variables over a configuration file over defaults. Parse once, reject unknown or invalid values, and log the safe effective configuration. Environment variables arrive as strings: `bool("false")` is `True`, so parse booleans using an explicit accepted vocabulary. Validate positive sizes and deadlines before opening resources.

Configuration should select supported behavior rather than evaluate arbitrary expressions or import untrusted plugins. Do not turn a straightforward pipeline into an undocumented configuration language. Version the schema when config changes can alter output semantics.

### Error classification, retries, and exponential backoff

| Failure | Example | Response |
| --- | --- | --- |
| Transient infrastructure issue | Connection reset, a retryable service error | Retry within a bounded budget if the operation is safe |
| Throttling | Documented quota response | Respect server guidance and reduce shared request rate |
| Invalid input | Broken date, unknown schema version | Reject/quarantine according to policy; retrying unchanged input does not fix it |
| Configuration/permission error | Invalid credential or forbidden destination | Fail with actionable context; do not repeat indefinitely |
| Ambiguous write result | Timeout after sending a commit request | Check outcome or replay with an idempotency strategy |

A common backoff cap for attempt `k` starting at zero is `min(max_delay, base_delay * 2**k)`. **Full jitter** chooses a random delay between zero and that cap to spread synchronized clients. Also bound attempts and total elapsed time, account for operation timeouts, and respect a valid `Retry-After` instruction within the overall deadline.

Use the SDK's existing retry policy when suitable. Layered retries multiply: three application attempts each invoking three SDK attempts can produce nine requests. Do not broadly retry every `Exception`: malformed SQL, validation errors, and programmer mistakes usually need correction. A write with a timeout may have succeeded, so retry safety depends on operation semantics rather than the exception class alone. [HTTP retry semantics](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2).

### Idempotency and fault tolerance

An operation is **idempotent** when repeating it has the same intended observable effect as doing it once. Overwriting a deterministic partition with identical validated content can be idempotent; appending the same rows on every attempt is not. “The scheduler runs only once” is not an idempotency strategy.

For a database-backed loader:

1. Read a deterministic source interval or page.
2. Validate and write using a stable event key, source position, or batch identifier with enforced uniqueness or an equivalent atomic merge.
3. Commit output durably.
4. Advance the checkpoint only after output success, ideally in the same transaction if both live in the same database.

If the process crashes between steps 3 and 4, it replays output; deduplication or upsert semantics must make that safe. If it advances the checkpoint first, a crash can lose records permanently. For independent systems, coordinate through an appropriate transactional/outbox design or accept replay plus reconciliation; a local `try/except` cannot create a distributed transaction.

Fault tolerance also needs durable input or replay, bounded retries, explicit terminal states, and recovery visibility. An in-memory queue and a restarted process cannot recover records that existed nowhere else.

### Logging, observability, and structured exceptions

Logs explain individual events; metrics show trends and alert conditions; traces connect work across service boundaries. Include safe context such as run ID, source partition, batch ID, attempt, row counts, bytes, elapsed time, and checkpoint. Measure queue depth, retry rate, rejected rows, lag, and downstream latency rather than only “job succeeded.”

```python
import logging

logger = logging.getLogger(__name__)

class BatchLoadError(Exception):
    def __init__(self, batch_id):
        self.batch_id = batch_id
        super().__init__(f"Batch {batch_id} failed to load")

def load_batch(writer, rows, batch_id):
    try:
        writer(rows)
    except OSError as exc:
        raise BatchLoadError(batch_id) from exc
    logger.info("batch_loaded", extra={"batch_id": batch_id, "rows": len(rows)})
```

This adapter assumes its writer uses `OSError` for the failure being translated; real database adapters should catch their documented exception types. Configure handlers once at the entry point. `extra` attaches fields to a `LogRecord`; emitting them as JSON requires a formatter or logging setup that serializes those fields. Use `logger.exception` at the boundary responsible for reporting the failure and avoid duplicate tracebacks at every layer. [Logging guide](https://docs.python.org/3.14/howto/logging.html).

Exclude credentials, raw personal data, SQL parameter values, and signed URLs. Put high-cardinality record IDs in controlled logs or traces, not metric labels that can create millions of series. Exception messages should identify the failed operation without dumping an entire sensitive payload.

**Interview questions**

- **Where should retries live?** At a boundary that understands transient failures, retry budgets, and side effects; coordinate with lower-level retries.
- **What makes dependency injection useful in a pipeline?** It separates business rules from external services and allows deterministic failure tests.
- **Can retries provide exactly-once output?** No. Retries need an idempotent or transactional destination contract to prevent repeated side effects.
- **What should a failed batch log?** Stable identifiers, safe error classification, attempt, elapsed time, and recovery position, with sensitive data excluded.

## 8. Testing & Code Quality

### Unit, integration, end-to-end, and contract tests

A transformation can pass every isolated test and still fail because the real driver buffers an entire result or the database commits differently than expected. Different test levels answer different questions. Use small deterministic checks for local behavior, then exercise the real boundaries where a wrong assumption could lose or duplicate data.

| Test level | Checks | Example | Main limitation |
| --- | --- | --- | --- |
| Unit | One behavior with controlled dependencies | Amount validation, checkpoint comparison | Cannot prove a real driver or service behaves as assumed |
| Integration | A real boundary between components | Transaction rollback against the target database | Needs a controlled environment and cleanup |
| End-to-end | A complete representative workflow | Source extraction through published data and checkpoint | Slower; many causes can produce one failure |
| Contract | Producer/consumer expectations at a boundary | API schema, field nullability, timezone and decimal representation | A compatible shape alone does not prove correct business meaning |

Put many edge cases in fast deterministic unit tests and fewer high-value tests around real boundaries. For ingestion, particularly valuable cases are replay, empty input, duplicate keys, a partial write, an ambiguous commit, bad records, schema drift, and cancellation before checkpoint advancement.

### pytest, fixtures, and parametrization

Pytest discovers test functions and provides assert introspection. Fixtures supply reusable setup and dependencies by name; parametrization runs the same behavior against several cases. Use the narrowest useful fixture scope to avoid state leaking between tests.

Save this complete example as `test_orders.py` in an environment with pytest installed, then run `python -m pytest -q test_orders.py`:

```python
import pytest

def validate_amount(value):
    if type(value) is not int or value < 0:
        raise ValueError("amount_cents must be a nonnegative integer")
    return value

@pytest.mark.parametrize("value", [0, 1, 10_000])
def test_valid_amount(value):
    assert validate_amount(value) == value

@pytest.mark.parametrize("value", [-1, "100", None, True, 1.5])
def test_invalid_amount(value):
    with pytest.raises(ValueError, match="amount_cents"):
        validate_amount(value)

@pytest.fixture
def source_file(tmp_path):
    path = tmp_path / "orders.txt"
    path.write_text("100\n200\n", encoding="utf-8")
    return path

def test_read_amounts(source_file):
    with source_file.open(encoding="utf-8") as source:
        actual = [validate_amount(int(line)) for line in source]
    assert actual == [100, 200]
```

Fixtures that acquire resources can `yield` and clean up in `finally`. Establish cleanup immediately after acquisition, so a later setup failure does not leak the resource. `tmp_path` provides an isolated temporary directory. Module/session-scoped fixtures are cheaper to share but need deliberate isolation of mutable database state. [Pytest fixtures](https://docs.pytest.org/en/stable/how-to/fixtures.html).

### Mocking, monkeypatching, and dependency injection

Prefer passing a small fake dependency when it expresses the behavior clearly. A fake writer can record accepted batches or raise on the second write, letting a test verify checkpoint behavior without patching implementation internals.

`unittest.mock` can assert interactions and simulate failures. Patch a name where the tested module looks it up: if `loader.py` imported `get` directly, patch `loader.get`, rather than assuming a patch of the original module's `get` changes that binding. `autospec` helps detect invalid call signatures but cannot prove that a service accepts the payload.

Pytest's `monkeypatch` temporarily changes attributes, environment variables, or paths and restores them after the test. It is useful for an environment-configured entry point. Inject a clock or sleep function when testing time-dependent retries; real sleeping makes tests slow and flaky. [Mock patching](https://docs.python.org/3.14/library/unittest.mock.html#where-to-patch), [Pytest monkeypatch](https://docs.pytest.org/en/stable/how-to/monkeypatch.html).

Do not mock the very behavior under test: mocking `commit` to always succeed cannot prove transactional atomicity. Use a real database integration test to verify rollback, constraints, and connection lifecycle.

### Property-based and contract testing

Property-based testing generates many inputs and checks an invariant, then shrinks failures to simpler examples. Hypothesis can explore empty collections, unusual Unicode, numeric boundaries, and combinations that hand-selected examples miss.

This independent example requires Hypothesis. It checks that stable deduplication preserves first occurrences, removes duplicates, and is idempotent:

```python
from hypothesis import given, strategies as st

def deduplicate(values):
    return list(dict.fromkeys(values))

@given(st.lists(st.integers()))
def test_deduplicate(values):
    actual = deduplicate(values)
    assert set(actual) == set(values)
    assert len(actual) == len(set(values))
    assert [values.index(value) for value in actual] == sorted(
        values.index(value) for value in actual
    )
    assert deduplicate(actual) == actual
```

For serialization, test a round trip only within the supported domain: JSON does not preserve every Python type or every numeric interpretation across consumers. For incremental loading, a useful property is that replaying an accepted batch does not change final business output. [Hypothesis quickstart](https://hypothesis.readthedocs.io/en/latest/quickstart.html).

Contract tests should include field presence versus nullability, decimal scale, timestamp zone, identifier representation, allowed enum values, and compatibility across producer versions. A REST fixture that matches yesterday's response cannot replace monitoring and testing against supported provider behavior.

### Ruff, mypy, formatting, and static analysis

| Check | Finds | Does not prove |
| --- | --- | --- |
| Ruff linting | Selected correctness issues, unused imports, configured rule violations | Business semantics, data correctness, all security issues |
| Ruff formatting | Consistent source layout | Correct behavior |
| mypy | Static type inconsistencies in checked code | Valid external data or correct execution of untyped dependencies |
| Tests | Behavior for exercised examples/properties | All possible failures |
| Dependency/security scanning | Known issues within the scanner's coverage | Absence of unknown vulnerabilities or a secure architecture |

Typical CI commands for an application with these installed tools are `ruff check .`, `ruff format --check .`, `mypy src`, and `python -m pytest`. Match paths and settings to the repository. Ruff does not replace a full type checker; mypy's ability to detect problems depends on annotations, configuration, stubs, and treatment of `Any`. [Ruff](https://docs.astral.sh/ruff/), [Mypy getting started](https://mypy.readthedocs.io/en/stable/getting_started.html).

### Coverage, CI/CD, and pre-commit

Line coverage shows which lines ran; branch coverage also distinguishes conditional paths. A test can execute a line without asserting its outcome, so a high percentage is not proof of correctness. Prioritize checks for lost/duplicated records, partial side effects, error handling, and boundary validation.

A useful CI pipeline installs from the supported lock/constraints, checks formatting/types/tests, builds the artifact, and tests that artifact's import or entry point. Run integration tests with isolated credentials and fixtures. Keep deployment approval and secret access aligned with the environment; untrusted pull-request code should not receive production secrets.

Pre-commit hooks provide fast local feedback, such as formatting and linting. They can be skipped and do not replace CI. Pin hook/tool versions for consistent results and keep expensive external-system tests out of the fast local loop.

**Interview questions**

- **What should you mock in a transformation test?** The external I/O boundary, while executing the actual transformation and asserting its output.
- **How do you test a retry safely?** Inject deterministic failures, a fake sleep/clock where needed, and verify attempts, propagated terminal failure, and idempotent side effects.
- **What does 100% coverage miss?** Weak assertions, missing input domains, race conditions, and behavior outside the tested environment.
- **Why test replay explicitly?** A first successful run does not establish correctness after a crash between commit and checkpoint.

## 9. Packaging & Environments

### Modules, packages, and the import system

A job that imports correctly on your laptop can fail in a worker image because installing a distribution and importing a module are different operations. A **module** is an importable unit, often a `.py` file. A regular **package** groups modules and normally has an `__init__.py`; a namespace package can span locations without one. The distribution name used by a package manager need not match its import names, and one distribution can expose several packages.

On first import, Python finds a module through its import machinery, creates a module object, executes its top-level code, and caches it in `sys.modules`. Subsequent ordinary imports reuse the cached module. Expensive top-level work therefore affects startup, and circular imports can expose partially initialized modules.

`sys.path` determines module search locations; local files such as `json.py` or `typing.py` can shadow intended modules. Avoid manually modifying it to repair packaging mistakes. Use installed packages and explicit imports. Running `python -m package.module` preserves the package execution context and often avoids relative-import problems caused by running a nested file directly. [Import system](https://docs.python.org/3.14/reference/import.html).

The `if __name__ == "__main__":` guard distinguishes direct execution from ordinary import. Keep reusable functions importable without starting a job, opening connections, or launching processes. A `src/` layout can help ensure tests exercise installed code rather than accidentally importing a repository directory that is absent from the built artifact.

### Virtual environments and dependency isolation

A virtual environment provides an isolated Python package installation context. It does not isolate the operating system, network, filesystem, or native system libraries. Create one with `python -m venv .venv`; use its interpreter explicitly or activate it through your shell.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check
```

These commands assume an existing requirements file and a POSIX shell; Windows uses a different interpreter path. `python -m pip` selects pip associated with that interpreter, avoiding a common mismatch between `pip` and `python`. Do not commit the environment directory; recreate it from declared dependencies. [Virtual environments](https://docs.python.org/3.14/library/venv.html).

### pip, uv, Poetry, and dependency resolution

| Tool | Main responsibility | Reproducibility detail |
| --- | --- | --- |
| pip | Install distributions and resolve compatible requirements | A requirements file needs an explicit pinning/hash policy; pip alone does not make an arbitrary file a lock |
| uv | Environment, Python, dependency, and project workflows | Project workflows use `uv.lock`; `uv sync --locked` fails when that lock needs updating |
| Poetry | Project dependency management, environments, packaging | Commit `poetry.lock` for an application and install from it |

A resolver chooses versions satisfying the declared dependency graph, including transitive requirements, Python compatibility, markers, and available distributions. Resolution can succeed while the application still has a behavioral incompatibility, so exercise the chosen versions.

In uv, `--locked` verifies the lock matches project metadata; `--frozen` uses the existing lock without checking that it is current. Choose deliberately in CI rather than treating them as synonyms. Use one coherent project workflow instead of maintaining several independently generated locks. [Uv locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/), [Poetry basic usage](https://python-poetry.org/docs/basic-usage/).

### pyproject.toml, wheels, and source distributions

`pyproject.toml` can define build-system requirements, project metadata and dependencies, and tool configuration. It is not itself a resolved dependency lock. This illustrative configuration describes a package exposing `orders_pipeline.cli:main`; the named module and function would need to exist in a real project:

```toml
[build-system]
requires = ["setuptools>=77"]
build-backend = "setuptools.build_meta"

[project]
name = "orders-pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []

[project.scripts]
orders-pipeline = "orders_pipeline.cli:main"
```

A **wheel** is a built distribution that installs without running the project's source build. A **source distribution (sdist)** contains source and build metadata and must be built into an installable artifact. Native-extension wheels depend on platform, architecture, Python ABI, and supported compatibility tags; a wheel for an ordinary CPython build is not automatically compatible with a free-threaded build.

Build isolation installs declared build dependencies in a separate build environment. It does not make untrusted source builds safe: build backends execute code. Test the built wheel's import, entry point, and packaged resources; an editable development install can hide missing files or packaging errors. [Python packaging overview](https://packaging.python.org/en/latest/overview/), [Pyproject specification](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/).

### Version pinning and reproducible builds

Applications usually deploy a tested dependency resolution, including transitive versions. Libraries normally declare compatible ranges for consumers and test those supported ranges; forcing an application's entire lock into a reusable library's dependency metadata creates unnecessary conflicts.

Reproducibility also requires the intended Python version, platform, package sources, build tools, native libraries, and artifact integrity. Hash verification checks that an artifact matches an expected hash; it does not establish that the package is trustworthy. A lock does not guarantee byte-identical wheels when build timestamps or other nondeterministic inputs vary.

Keep lock updates reviewable and run compatibility/security checks when updating. Avoid uncoordinated public/private package sources that can resolve an internal name from an unintended index. Record the deployed artifact and runtime versions so an incident can be reproduced.

### Docker and application packaging

A container image packages an application and userspace dependencies; it shares the host kernel and is not a virtual machine. Use a compatible Python base, install from the tested dependency set, build application artifacts, and execute a clear entry point.

Build stages can keep compilers and build-only dependencies out of the runtime image. Native wheels still need compatible runtime shared libraries. Copy dependency metadata before frequently changing source files when useful for cache reuse. Use `.dockerignore` to exclude local environments, caches, data, and secrets; never bake credentials into image layers.

Run with an appropriate non-root identity, make writable locations explicit, and deliver termination signals to the application. Pin image digests when exact base identity matters, and update them through a controlled process. Test with actual CPU/memory limits because container limits can expose assumptions hidden on a developer laptop. [Docker build practices](https://docs.docker.com/build/building/best-practices/).

**Interview questions**

- **Why does a module import locally but fail in production?** Compare interpreter, installed artifact, dependencies, package discovery, working directory, and shadowing modules.
- **Is a requirements file reproducible?** Only to the extent that it fixes the relevant dependency graph, artifacts, sources, and compatible runtime; unconstrained entries can resolve differently.
- **Why test a wheel instead of only the checkout?** The wheel can omit a module, resource, or entry point that exists in the source tree.
- **Does Docker replace a dependency lock?** No. An image build that resolves floating dependencies can produce different results on different days.

## 10. Python for Data Engineering

### Database access, connection pooling, and transactions

Opening a database connection is only the beginning of a reliable write path. Python's DB-API supplies common concepts—connections, cursors, parameter binding, commits, and rollbacks—but drivers differ in placeholders, thread-safety, transaction defaults, bulk loading, and streaming behavior. Treat the selected driver's contract as part of the pipeline design instead of assuming drivers are interchangeable. [DB-API specification](https://peps.python.org/pep-0249/).

- Bind **values** with driver parameters; never interpolate untrusted values into SQL strings.
- Identifiers such as table names generally cannot be value parameters. Use an allowlist and the driver's identifier-composition API.
- A connection pool reuses a bounded set of connections. Set acquisition timeouts and return connections in a clean transaction state.
- Each process should normally create its own pool after startup. Do not share inherited live connections across process boundaries.
- A transaction groups operations atomically under the database's isolation rules. Atomicity does not automatically prevent every concurrent anomaly.
- `fetchmany` bounds rows delivered per call, but a driver may already have buffered the complete result. Check server-side cursor/streaming support.

This self-contained SQLite example demonstrates parameter binding, replay-safe insertion of immutable events, and rollback. It uses `closing` because a SQLite connection's transaction context manager does not close the connection:

```python
import sqlite3
from contextlib import closing

with closing(sqlite3.connect(":memory:")) as connection:
    connection.execute(
        "CREATE TABLE events (event_id INTEGER PRIMARY KEY, "
        "amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0))"
    )
    statement = (
        "INSERT INTO events VALUES (?, ?) "
        "ON CONFLICT(event_id) DO NOTHING"
    )
    batch = [(1, 100), (2, 200)]
    for _ in range(2):
        with connection:
            connection.executemany(statement, batch)
    assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2

    try:
        with connection:
            connection.executemany(statement, [(3, 300), (4, -1)])
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("The invalid amount should reject the transaction")
    assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
```

The event ID represents an immutable event: ignoring a conflict is valid only if repeated IDs have the same agreed meaning. Conflicting payloads need detection or an explicit update/version policy. This example does not implement source checkpoints or concurrent loaders. In a production loader, coordinate writes with checkpoint advancement as described in section 7. [SQLite transaction and context behavior](https://docs.python.org/3.14/library/sqlite3.html).

Batch inserts or database-native bulk loading reduce network round trips. Very large transactions increase lock duration, memory, and recovery cost; choose batch boundaries that preserve required atomicity. Connection pools manage connections, not unlimited database capacity.

### Batch processing, streaming, and pagination

**Batch** processing operates over a bounded input interval. **Streaming** processes continually arriving data with explicit state and progress tracking. A streaming source can still deliver microbatches. Define latency, ordering, replay retention, and how late or out-of-order events affect output.

Pagination retrieves a larger logical result through smaller requests:

| Method | Advantage | Risk |
| --- | --- | --- |
| Offset/limit | Simple and easy to inspect | Large offsets can be expensive; concurrent changes can shift rows between pages |
| Keyset | Uses a stable ordered key to continue | Needs a total ordering and appropriate index; mutations still require a source consistency policy |
| Opaque cursor/token | Follows the service's continuation contract | Tokens can expire and may not be durable checkpoints |

For keyset pagination ordered by `(updated_at, id)`, continue after the last complete tuple rather than using the timestamp alone. Specify a deterministic tie-breaker. A snapshot or fixed extraction upper bound helps define completeness while the source changes. Follow provider tokens as documented, and validate a returned next URL before sending credentials to a different host.

Persist progress only after the corresponding destination data is durable. An empty page does not necessarily mean completion if the API also returns a continuation token; follow its documented contract.

### REST APIs, HTTP clients, and rate limiting

Reuse a client/session to reuse connections. Configure connect, read, write, and connection-pool timeouts as supported, plus a total job/request deadline where needed. A read timeout often limits inactivity between reads, not the total duration of an arbitrarily long response. [HTTPX timeout model](https://www.python-httpx.org/advanced/timeouts/).

Validate status, content type when meaningful, payload size, JSON structure, and business fields before loading. Handle authentication expiry, documented throttling responses, retryable service failures, and ambiguous writes separately. Streaming an HTTP body avoids loading it all at once only if downstream processing also stays bounded; close the response even if parsing fails.

Use a token bucket, leaky bucket, or provider-supported scheduling policy to control request rate; use a semaphore to limit simultaneous requests. Account for rate limits across workers sharing a credential, not just inside one process. Honor applicable `Retry-After` guidance and keep a bounded retry budget. Retrying a POST may duplicate a side effect unless the endpoint provides a suitable idempotency contract.

### Cloud SDKs, object storage, and multipart uploads

An SDK handles service-specific signing, pagination, retries, and transfer APIs; it does not remove the need to understand service semantics. Configure its existing mechanisms before adding wrappers, and check whether clients are safe to reuse across threads. Prefer runtime identity and short-lived credentials over embedded long-lived keys.

For object storage, distinguish an object key, an object version, and any transactional table metadata above the objects. A prefix is not necessarily a real directory. Multi-object publication is not automatically atomic, and an operation named “rename” may be implemented as copy plus delete. Confirm the chosen service's actual guarantees.

Multipart upload divides one object into service-defined parts. Retain upload/part identifiers when resumability is required, retry parts using the service's contract, complete the upload only after all required parts succeed, and abort abandoned uploads or apply cleanup policies. Bound concurrent parts because each can consume buffers and connections. Use documented checksum support; do not assume an ETag is always the whole object's MD5.

Publishing a dataset can require more than finishing individual uploads: write immutable data, verify it, then expose a manifest or commit table metadata through a supported atomic mechanism. Readers should consume only committed datasets. SDK compatibility, limits, consistency, checksums, and multipart rules are provider-specific; verify them for the selected deployment.

### Airflow operators, hooks, and custom integrations

An Airflow **operator** defines task behavior; a **hook** encapsulates access to an external system, often through Airflow connections. Prefer a suitable existing provider operator/hook before implementing a custom integration. A custom operator should keep orchestration concerns thin and delegate reusable business logic to testable Python code.

Avoid network requests or expensive work at DAG import time or in operator initialization: the scheduler repeatedly parses DAGs. Create external clients during execution, acquire resources deliberately, and release them on failure. Airflow 3 uses public SDK interfaces; imports and execution constraints differ from Airflow 2, so use the installed major version's documentation. [Custom operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html).

Use task data intervals or explicit source positions to define inputs rather than the worker's current wall clock. A backfill should reconstruct the intended historical interval. Task retries imply repeated attempts, so a destination write still needs idempotency. Use XCom for modest coordination metadata such as paths or IDs, rather than moving huge DataFrames between tasks. Connection/secret handling, pools, retries, and concurrency should use the deployment's existing mechanisms.

### ETL/ELT and schema validation

ETL transforms before loading the analytical destination; ELT loads and then transforms in that destination. Python may orchestrate either. Push suitable joins and aggregation to the database or execution engine rather than pulling all records into Python simply to reproduce SQL.

Validate at the trust boundary and distinguish several contracts:

| Contract | Example |
| --- | --- |
| Structural | The payload is an object with required keys |
| Type | `amount_cents` is an integer, with booleans rejected if inappropriate |
| Semantic | Currency is supported, amount is within business bounds |
| Relational | An order references a valid customer |
| Temporal | Timestamps have an agreed timezone and source position |
| Evolution | New fields, unknown enums, and type changes follow a compatibility policy |

Type hints are not a schema validator. Use explicit checks or an already-adopted validation library, define coercion carefully, and record safe rejection metadata. A field silently converted from an invalid string to zero can make a successful job publish incorrect revenue.

### Large files and memory-efficient ingestion

Match the reader to the format. JSON Lines permits record-by-record parsing; a giant JSON array generally needs a suitable streaming parser or a different source format. Use the CSV module for quoted fields and embedded newlines rather than `line.split(",")`. Compressed files can expand dramatically, so bound decoded records and work as well as compressed input bytes.

The following JSON Lines reader uses a bounded binary `readline` before decoding, avoiding allocation of an arbitrarily long input line. It requires UTF-8 JSON objects with nonnegative integer IDs and amounts; blank lines are rejected.

```python
import json

def iter_orders(path, max_line_bytes=1_048_576):
    if max_line_bytes <= 0:
        raise ValueError("max_line_bytes must be positive")
    with open(path, "rb") as source:
        line_number = 0
        while raw := source.readline(max_line_bytes + 1):
            line_number += 1
            if len(raw) > max_line_bytes:
                raise ValueError(f"Line {line_number} exceeds the byte limit")
            try:
                record = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid JSON on line {line_number}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Expected an object on line {line_number}")
            for field in ("order_id", "amount_cents"):
                value = record.get(field)
                if type(value) is not int or value < 0:
                    raise ValueError(f"Invalid {field} on line {line_number}")
            yield record
```

The limit includes a line terminator when present. This bounds line input size, not every parser allocation, nesting depth, downstream buffer, or decompression ratio. A stricter external-input policy may require additional parser/resource limits. Since this is a generator, consume it within a lifetime that guarantees completion or explicitly close it if stopping early.

For Parquet, read selected columns in record batches instead of materializing the entire table. This example requires PyArrow and a compatible `orders.parquet` file:

```python
import pyarrow.parquet as pq

with pq.ParquetFile("orders.parquet") as source:
    for batch in source.iter_batches(
        batch_size=65_536, columns=["order_id", "amount_cents"]
    ):
        assert batch.num_rows <= 65_536
```

The batch size limits rows emitted, not an absolute memory ceiling for decoding and prefetching. Converting every batch to Python dictionaries adds object overhead. Keep columnar operations where possible. [PyArrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html).

### Data quality, reconciliation, and incremental loading

Data quality checks include schema, required fields, uniqueness, value ranges, referential integrity, freshness, and unexpected distribution changes. Classify which failures block publication and which are quarantined. Record accepted and rejected counts so dropping bad rows cannot silently make the run look complete.

**Reconciliation** compares source and target at matching grain and boundaries: counts, monetary totals, minimum/maximum source positions, null counts, and appropriate checksums. A matching row count alone cannot detect one missing row replaced by one duplicate. Hash reconciliation requires canonical representation, ordering rules, and a suitable collision policy.

Incremental strategies include monotonically increasing IDs, timestamp watermarks, change data capture (CDC), and source offsets. A timestamp watermark needs tie-breaking and a late-arrival policy; updates and deletes may require CDC or periodic reconciliation. A common approach rereads an overlap window and deduplicates by stable key/version, accepting extra work to reduce missed late updates.

Separate **event time**, **source modification/commit time**, and **ingestion time**. An old event arriving today will be missed if the incremental predicate uses event time but assumes arrival order. Checkpoint only contiguous successfully processed source positions; taking the largest completed offset while lower offsets are unfinished can skip data.

**Interview questions**

- **How would you ingest a 200 GB JSON Lines file on a small worker?** Stream bounded records, validate, batch writes, avoid retaining results, and design replay/checkpoint behavior.
- **How do you avoid losing rows with the same update timestamp?** Use a stable tie-breaker and an explicit source consistency/late-arrival policy.
- **What happens if a task crashes after a database commit?** The next attempt may replay; stable keys and transactional progress tracking must make that safe.
- **Can an Airflow retry fix an invalid schema?** Repeating the same input does not repair it; classify the error and surface a useful failure or quarantine result.

## 11. Production & Advanced Topics

### Async I/O versus multiprocessing in a mixed pipeline

A real ingestion job rarely has just one kind of work: it waits for I/O, decodes responses, normalizes values, and invokes native writers. Concurrency helps only when it addresses the stage that is limiting progress. CPU-heavy Python code on an async event-loop thread can erase the benefit of concurrent extraction; sending tiny records individually to a process pool can cost more than local processing. Profile the stages before combining concurrency models.

Batch sufficiently expensive CPU work, minimize serialized payloads, and bound every stage's in-flight work. A synchronous system with a small thread pool may be easier to operate than an async/process hybrid with the same throughput. Reuse a native engine's execution facilities before layering additional pools around it.

Python 3.14 also provides `InterpreterPoolExecutor`: workers have separate interpreters and can execute across cores, with isolated runtime state and serialization boundaries. It does not provide arbitrary shared mutable Python objects; check extension compatibility and actual transfer costs. This is another option to measure, not a universal replacement for processes. [Interpreter pools](https://docs.python.org/3.14/library/concurrent.futures.html#interpreterpoolexecutor).

### Memory leaks and resource management

Not every rising RSS graph is a leak. Distinguish retained live objects, temporary peaks, allocator reuse, native buffers, and genuinely unreachable native allocations.

| Symptom | Plausible cause | Useful check |
| --- | --- | --- |
| Growth proportional to processed keys | Unbounded cache or deduplication set | Track cache/key cardinality and retained references |
| Growth while the sink slows | Unbounded queue or futures/results | Measure queue depth and in-flight bytes |
| Stable Python allocations, growing RSS | Native buffers, driver allocations, allocator behavior | Compare process/native metrics and component lifetimes |
| Open files/connections steadily increase | Missing cleanup or abandoned iterators/responses | Track descriptor counts, pool usage, and cancellation paths |
| High baseline after a large batch | Allocator retention or fragmentation | Repeat a controlled workload and compare later peaks |

Generators retain local references while suspended; exception tracebacks can retain frames; closures and global registries can retain whole batches. Use bounded caches and explicit lifetimes. Weak references can express non-owning relationships where suitable, but are not a replacement for deciding who owns and releases resources.

Close files, responses, clients, and pools deterministically. When using shared memory or temporary files, define which process creates, closes, and removes them, including failure recovery. A successful normal shutdown test does not cover a worker killed while holding resources.

### Profiling CPU, memory, and I/O bottlenecks

Start with one reproducible workload and a stage timeline: read, decode, validate, transform, write, commit, checkpoint. Record throughput, latency percentiles, CPU, memory, bytes, queue depth, and service timings.

- High CPU with low throughput: inspect hot code, parsing, repeated conversions, algorithmic complexity, and native thread counts.
- Low CPU with long elapsed time: inspect network/storage waiting, database locks, connection acquisition, throttling, and serial dependencies.
- Throughput collapses as workers increase: inspect memory pressure, downstream saturation, serialization, lock contention, and oversubscription.
- Memory spikes during a join: inspect duplicate key multiplicity, intermediate copies, and the required output size before tuning allocators.

Change one material factor at a time and compare correctness as well as runtime. Use representative sizes and skew; a uniform thousand-row fixture cannot reveal a hot partition or millions of distinct groups. Separate startup cost from steady-state cost according to how the production job actually runs.

### Serialization and inter-process communication

Process boundaries exchange representations, not shared references to ordinary objects. A large DataFrame can incur serialization, transfer, deserialization, and duplicated memory before useful work begins. Passing a filename and partition range can be cheaper when workers can read independently, but it also changes I/O load and consistency requirements.

`multiprocessing.Queue` normally serializes messages. Shared memory or memory mapping can reduce bulk copies for suitable layouts, but requires explicit schema, synchronization, and lifecycle ownership. Arrow IPC provides a columnar exchange representation; zero-copy behavior still depends on the transport and consumer. Sending tiny per-row messages can be dominated by scheduling and framing overhead; batch to a measured size.

Pickled functions also need importable code and compatible dependencies in workers. Do not include secrets or unnecessary clients in captured closures. A process-local global counter is not a distributed progress tracker, and a thread lock does not synchronize separate processes.

### Resilient distributed task execution

Assume a worker may disappear after performing a side effect but before acknowledging success. A typical durable task queue therefore permits redelivery; consumers need an idempotency or transaction strategy.

1. Accept work with a stable task/batch identity and bounded input.
2. Acquire any required lease or ownership under the system's protocol.
3. Perform a replay-safe write and persist the result/progress.
4. Acknowledge completion only after the required durable effects.
5. On terminal invalid input, retain safe failure metadata and a controlled replay/quarantine path.

Lease expiry does not prove the previous worker stopped. If overlapping owners can corrupt output, use destination-enforced fencing/version checks or an equivalent concurrency mechanism. Deduplication keys need a defined retention window; expiring them before old work can be redelivered reintroduces duplicates.

Handle poison messages without endless hot-loop retries, expose retry/age/dead-letter metrics, and reconcile uncertain outcomes. “Exactly once” must name its scope: a transactional sink can guarantee one logical result under a defined protocol while unrelated external notifications still need their own coordination.

### Security: secrets, deserialization, and injection

| Boundary | Unsafe pattern | Safer approach |
| --- | --- | --- |
| SQL values | String interpolation of external fields | Driver parameter binding |
| SQL identifiers | Accept arbitrary table/column names | Allowlist plus supported identifier quoting |
| Shell commands | Concatenate input and use `shell=True` | Argument list with `shell=False`; validate supported operations |
| Serialization | Load untrusted pickle or unsafe YAML constructors | A constrained format, safe loader, schema and size limits |
| Dynamic code | `eval`, arbitrary imports from external config | Explicit supported operations and validated configuration |
| Credentials | Commit keys or emit tokens in logs | Runtime secret retrieval, restricted access, redaction |
| HTTP destinations | Fetch arbitrary input URLs with privileged credentials | Validate destinations, redirects, credential scope, and network access |
| File paths/archives | Trust absolute paths or traversal entries | Constrain destinations and safely validate extraction paths |

TLS verification should remain enabled for remote services, and clients need timeouts and response-size controls. Treat “internal” data as an input boundary when producers can change independently. Safe JSON parsing does not prevent decompression bombs, extreme nesting, or business-invalid data.

Use the least privileges required for source reads and destination writes. A Python virtual environment is not a sandbox for untrusted code; imports, builds, plugins, and annotation evaluation can execute code. Security controls belong at the actual execution and data boundaries, not only in type annotations.

### Debugging production failures and tuning safely

First preserve evidence: run ID, source interval, deployment/artifact version, effective safe configuration, checkpoint, error chain, and destination commit state. Determine whether the failure is deterministic input/code, environment, capacity, concurrency, or an external dependency. Check whether a restart can duplicate effects before replaying.

| Failure | First investigation | Recovery consideration |
| --- | --- | --- |
| Job terminated without a Python traceback | Container/OS exit reason, memory limits, worker logs | Was the batch committed or still pending? |
| Task hangs | Thread/task state, blocked I/O, pool waits, lock order | Cancellation may not stop an underlying thread or remote write |
| Data duplicated after retry | Stable key and destination uniqueness/merge semantics | Reconcile existing output before replay |
| Incremental rows missing | Predicate ordering, tie-breaker, late data, checkpoint order | Rewind a bounded source interval and deduplicate safely |
| Failure only after deployment | Built artifact, runtime/dependency changes, ABI and permissions | Roll back a compatible artifact/configuration pair |
| Correctness changes under load | Shared state, transaction isolation, ownership races | Reproduce with controlled concurrency and a concrete invariant |

Python's `faulthandler` can help obtain thread tracebacks for hangs or fatal faults; enable it through a supported diagnostic setup before an incident when possible. `breakpoint()` is useful in a controlled reproduction, but an interactive stop in a production worker can hold locks and leases. Capture stack traces and sanitized samples without exposing credentials or raw personal data. [Faulthandler](https://docs.python.org/3.14/library/faulthandler.html).

After a fix, validate the original failure case and a representative normal run, then compare output reconciliation and resource use. A throughput improvement that changes decimal rounding, null handling, ordering requirements, or checkpoint semantics is a correctness change and needs explicit review.

### Interview practice scenarios

1. **A Python ingestion job grows from 300 MB to 8 GB over six hours.** Separate live Python retention from native memory and allocator behavior; inspect queues, caches, result accumulation, and repeated-batch snapshots before changing GC settings.
2. **An API permits 100 requests per minute, but your 10-worker job gets throttled.** Concurrency is not rate; coordinate the shared rate budget, respect provider instructions, and bound retries and pending work.
3. **A loader writes successfully, times out, and then produces duplicates.** The first outcome was ambiguous. Use stable write identities, destination enforcement, and checkpoint/reconciliation semantics.
4. **A CPU-heavy transformation became slower after adding 16 processes.** Measure serialization, task granularity, worker startup, memory pressure, and nested native thread pools before increasing workers again.
5. **A generator-based pipeline still runs out of memory.** Trace every materialization and retained reference, including grouping state, futures, queues, output buffers, and exceptionally large records.
6. **A type-checked pipeline accepts a payload with `amount_cents=True`.** Static annotations did not validate runtime JSON, and `bool` is an `int` subclass. Apply the actual field contract at ingestion.
7. **An incremental load skips two records with the same timestamp.** Inspect total ordering and checkpoint predicates; use the full continuation key and define a source-consistency/late-data policy.
8. **An async task is cancelled while a database write runs in a thread.** Cancellation of the await does not establish that the write stopped; determine the destination outcome before replay.
9. **A backfill returns different output from the original run.** Check source snapshots, schema/code versions, reference data, time-dependent configuration, and deterministic interval definitions.
10. **You are asked whether free-threaded Python replaces multiprocessing.** Compare dependency support, shared-state safety, isolation, memory, transfer overhead, and measured scaling for the specific workload.
