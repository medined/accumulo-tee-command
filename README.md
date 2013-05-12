accumulo-tee-command
====================

A tee command for the accumulo shell - very experimental - very inefficient

An afternoon project. There are hacks involved. Do not use in production!

Some of the iterators that I've been writing are designed to create a problem-oriented dataset; a limited view into the larger dataset. Once the iterators are put into place in the shell, there isn't a way to easily materialize that sub-set of the data. I'm not even sure it makes sense to materialize it, but it was interesting to experiment with the code.

Add these three files to the Accumulo v1.4.3 source code, re-compile and re-install in order to 
get a tee command.

Once inside the shell you can use tee like this:

> table myTable
myTable> tee myTableCopy on
myTable> scan
row cf cq v
myTable> tee myTableCopy off
myTable> table myTableCopy
myTableCopy> scan
row cf cq v

Remember to turn the tee off.
