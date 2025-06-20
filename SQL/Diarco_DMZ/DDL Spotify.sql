CREATE DATABASE Spotify; --creo DBUSE Spotify; --cambio a uso DBCREATE SCHEMA base1; --creo schema-----comienzo a crear tablas-------CREATE TABLE base1.tipo_usuario (	id_suscripcion INT NOT NULL,	descripcion_suscripcion VARCHAR(50) NOT NULL,	CONSTRAINT pk_id_suscripcion PRIMARY KEY (id_suscripcion));

CREATE TABLE base1.usuario(	id_usuario INT NOT NULL,	nombre_usuario VARCHAR (50) NOT NULL,	apellido_usuario VARCHAR (50) NOT NULL,	shortname_usuario VARCHAR (50) NOT NULL,	id_suscripcion INT NOT NULL,	CONSTRAINT pk_id_usuario PRIMARY KEY (id_usuario),	CONSTRAINT fk_id_suscripcion FOREIGN KEY (id_suscripcion) REFERENCES base1.tipo_usuario (id_suscripcion))

CREATE TABLE base1.artista (	id_artista INT NOT NULL,	nombre_artista VARCHAR(50) NOT NULL,	CONSTRAINT pk_id_artista PRIMARY KEY (id_artista));
CREATE TABLE base1.album(	id_album INT NOT NULL,	nombre_album VARCHAR (100) NOT NULL,	id_artista INT NOT NULL,	CONSTRAINT pk_id_album PRIMARY KEY (id_album),	CONSTRAINT fk_id_artista FOREIGN KEY (id_artista) REFERENCES base1.artista (id_artista))
CREATE TABLE base1.autor (
	id_autor INT NOT NULL,
	nombre_autor VARCHAR(50) NOT NULL,
	CONSTRAINT pk_id_autor PRIMARY KEY (id_autor)
);


CREATE TABLE base1.podcast(
	id_podcast INT NOT NULL,
	duracion FLOAT NOT NULL,
	id_autor INT NOT NULL,
	CONSTRAINT pk_id_podcast PRIMARY KEY (id_podcast),
	CONSTRAINT fk_id_autor FOREIGN KEY (id_autor) REFERENCES base1.autor (id_autor)
)

CREATE TABLE base1.cancion (
	id_cancion INT NOT NULL,
	nombre_cancion VARCHAR(50) NOT NULL,
	duracion FLOAT NOT NULL,
	CONSTRAINT pk_id_cancion PRIMARY KEY (id_cancion)
);

CREATE TABLE base1.lista_reproduccion(
	id_lista_reproduccion INT NOT NULL,
	nombre_lista VARCHAR(50) NOT NULL,
	id_usuario INT NOT NULL,
	CONSTRAINT pk_id_lista_reproduccion PRIMARY KEY (id_lista_reproduccion),
	CONSTRAINT fk_id_usuario FOREIGN KEY (id_usuario) REFERENCES base1.usuario (id_usuario)
)

CREATE TABLE base1.album_cancion (
	id_album INT NOT NULL,
	id_cancion INT NOT NULL,
	CONSTRAINT pk_id_album_cancion PRIMARY KEY (id_album, id_cancion),
	CONSTRAINT fk_id_album FOREIGN KEY (id_album) REFERENCES base1.album (id_album),
	CONSTRAINT fk_id_cancion FOREIGN KEY (id_cancion) REFERENCES base1.cancion (id_cancion)
);