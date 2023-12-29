# jepsen.ravendb

A simple test for single-node deployments of RavenDB.

## Quickstart

To reproduce lost update by defaul, try:

```
lein run test --nodes n1 --concurrency 2n --rate 10000 --time-limit 5
```

To reproduce G-single with cluster-wide transactions, try:

```
lein run test --nodes n1 --concurrency 5n --rate 10000 --time-limit 10 --txn-mode cluster-wide
```

## License

Copyright © 2023 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
