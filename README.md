s3fs
====

An S3 Filesystem Provider for Java 7

Implementación muy básica y poco optimizada... pero "completa".

Features:

* Crear directorio y ficheros
* Borrar directorios y ficheros
* Copiar entre Paths con distintos providers
* Recorrer ficheros de un directorio.

Roadmap:

* Muchos mas tests unitarios (better test coverage)
* No permitir subir ficheros con mismo nombre que folders y viceversa (Disallow upload binary files with same name as folders and vice versa)
* Decidir que hacer con FileSystemProvider. ¿se pueden crear varios? ¿por bucketname, en vez de por endpoint?
* Utilizar Multipart para una mejor implementación del outputstream
* ¿alguna idea para los seekable?

Fuera del Roadmap:

* Watchers
* FileStore