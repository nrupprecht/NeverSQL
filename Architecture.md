# Architecture

Diagramming some of the architecture so far / my thoughts on what to do next.

```plaintext
-------------------
|                 | <----> Primary index
|   DataManager   | <----> Other indices
|                 |
-------------------
         |
         |
         V
-------------------         -----------------
|    PageCache    |  <----> | WriteAheadLog |
-------------------         -----------------
         |
         |
         V
-------------------         ------------
| DataAccessLayer |  <----> | FreeList |
-------------------         ------------
```

* **DataManager**: Resonsible for maintaining indices (B-trees) and other data within pages. It also provides an
  interface
  to access and modify data.
* **PageBuffer**: A buffer to cache pages in memory. Manages all access to physical pages, including what to cache and
  when.
* **DataAccessLayer**: Mechanism to load, store, and create pages in physical memory. Maintains structures like the free
  list to help it organize the physical memory. Controls the placement of pages in physical memory.