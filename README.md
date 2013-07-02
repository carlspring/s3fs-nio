An Amazon AWS S3 FileSystem Provider for Java 7 (NIO2)

Amazon Simple Storage Service provides a fully redundant data storage infrastructure for storing and retrieving any amount of data, at any time.
NIO2 is the new file management API, introduced in Java version 7. 
This project provides a first API implementation, little optimized, but "complete" to manage files and folders directly on Amazon S3.

<img src="https://travis-ci.org/jarnaiz/Amazon-S3-FileSystem-NIO2.png" alt="Travis CI status"/>

*Features*:

* Crear directorio y ficheros
* Borrar directorios y ficheros
* Copiar entre Paths con distintos providers
* Recorrer ficheros de un directorio.
* Permite trabajar con directorios virtuales (folders que no existen como objetos en Amazon S3 y son subkeys de un elemento)

*Roadmap*:

* Performance issue (slow querys with virtual folders)
* Muchos mas tests unitarios (better test coverage)
* No permitir subir ficheros con mismo nombre que folders y viceversa (Disallow upload binary files with same name as folders and vice versa)
* Decidir que hacer con FileSystemProvider. ¿se pueden crear varios? ¿por bucketname, en vez de por endpoint?
* Utilizar Multipart para una mejor implementación del outputstream
* ¿alguna idea para los seekable?

*Fuera del Roadmap*:

* Watchers
* FileStore
