# Python System Design: SOLID Guided by DRY, KISS, and YAGNI

Before diving into the SOLID principles, it is critical to understand the three foundational philosophies of software design. In practice, blindly applying SOLID principles can lead to over-engineering. Therefore, SOLID should always be guided by—and sometimes restrained by—DRY, KISS, and YAGNI.

## The Foundational Guardrails

### 1. DRY (Don't Repeat Yourself)
**Concept:** Every piece of knowledge or logic must have a single, unambiguous, authoritative representation within a system. 
**In Python:** Avoid copy-pasting code. If you find yourself writing the same logic in multiple places, extract it into a function, module, or base class.

### 2. KISS (Keep It Simple, Stupid)
**Concept:** Systems work best if they are kept simple rather than made complicated. Simplicity should be a key goal in design.
**In Python:** Write readable, straightforward code. Don't use complex design patterns or deep inheritance trees if a simple function or a basic class gets the job done perfectly.

### 3. YAGNI (You Aren't Gonna Need It)
**Concept:** Always implement things when you actually need them, never when you just foresee that you need them.
**In Python:** Don't build extensive abstractions, generic interfaces, or future-proof hooks for features that *might* be requested next year. Build for today's requirements.

---

## The Balancing Act: Do SOLID and KISS/YAGNI Conflict?

**Yes, they can.** This is a common realization in Low-Level Design (LLD) interviews. 
Strict adherence to SOLID (especially the Open-Closed Principle and Dependency Inversion Principle) often requires creating Abstract Base Classes (ABCs), injecting dependencies, and splitting logic into many small files. If you do this from day one for a simple script, you are violating both **KISS** (the code is now overly complex) and **YAGNI** (you built abstractions you don't actually need yet).

**The Golden Rule:** Start with **KISS** and **YAGNI**. Write simple code. Don't repeat yourself (**DRY**). As the application grows and you find yourself constantly modifying existing files to add new features, *that* is the exact moment to refactor using **SOLID** principles.

---

## The SOLID Principles

With the guardrails in mind, here is a summary of the SOLID design principles in Python.

### 1. Single-Responsibility Principle (SRP)
**Definition:** A class should have only one reason to change, meaning it should focus on a single core responsibility or task. *(This directly supports DRY, as isolating responsibilities prevents overlapping logic).*

**Example:**
```python
# BAD: The class handles both file I/O and ZIP compression.
class FileManager:
    def read(self):
        pass
    def compress(self):
        pass

# GOOD: Responsibilities are split into dedicated classes.
class FileManager:
    def read(self):
        pass

class ZipFileManager:
    def compress(self):
        pass
```

### 2. Open-Closed Principle (OCP)
**Definition:** Software entities should be open for extension but closed for modification. *(Apply this only when you know a component will have many variations; otherwise, YAGNI dictates keeping it simple until needed).*

**Example:**
```python
from abc import ABC, abstractmethod

# GOOD: You can extend by simply adding a new class without modifying existing code.
class Shape(ABC):
    @abstractmethod
    def calculate_area(self):
        pass

class Circle(Shape):
    def __init__(self, radius):
        self.radius = radius
    def calculate_area(self):
        return 3.14 * self.radius ** 2
```

### 3. Liskov Substitution Principle (LSP)
**Definition:** Subclasses should be substitutable for their base classes without altering the correct behavior of the program.

**Example:**
```python
# BAD: A square overrides width/height simultaneously, breaking Rectangle's expected behavior.
class Rectangle:
    def __init__(self, width, height):
        self.width = width
        self.height = height

class Square(Rectangle):
    def __init__(self, side):
        super().__init__(side, side) # Violates the base class contract!

# GOOD: Use a common interface where both shapes define logic naturally.
class Shape(ABC):
    @abstractmethod
    def area(self): pass
```

### 4. Interface Segregation Principle (ISP)
**Definition:** Clients should not be forced to depend on interfaces (methods) that they do not use. *(A direct application of KISS—don't force complexity where it isn't needed).*

**Example:**
```python
# GOOD: Interfaces are segregated by capability rather than one massive class.
class Printer(ABC):
    @abstractmethod
    def print(self): pass

class Scanner(ABC):
    @abstractmethod
    def scan(self): pass

class SimplePrinter(Printer):
    def print(self): 
        print("Printing...")
```

### 5. Dependency Inversion Principle (DIP)
**Definition:** High-level modules should not depend on low-level modules. Both should depend on abstractions.

**Example:**
```python
# GOOD: Application relies on an abstraction, allowing easy substitution.
class Database(ABC):
    @abstractmethod
    def fetch_data(self): pass

class MySQLDatabase(Database):
    def fetch_data(self): pass

class App:
    # App accepts ANY database class that honors the Database interface
    def __init__(self, db: Database):
        self.db = db
```