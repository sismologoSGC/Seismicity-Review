![SGC](images/sgc_logo.png)<!-- .element width="700"-->

# Revisión de sismicidad

Rutina realizada para revisar la sismicidad.  

Se cuenta como error bajo las siguientes condiciones
```bash
1) Errores en latitud,longitud y profundidad > 11.9
2) Errores en rms >1.5
3) Eventos volcánicos no destacados que no tengan not_locatable
5) Eventos internacionales con M>5 sin asociar
6) Eventos destacados sin etiqueta de DESTACADO
```

## 1. Instalación en linux

### - Requerimientos previos
Se corre en sistemas linux.

### - Python
Python Versión 3.7 en adelante. (Usaremos como ejemplo python 3.8)
```bash
sudo apt-get install python3.7 (o 3.8)
```

Tener virtualenv en python.
```bash
python3.7 -m pip install virtualenv
```

#### Instalación con pip 
```bash
python3.7 -m virtualenv .revision
source .revision/bin/activate
pip install -r requirements.txt
```

## Instrucciones de uso

### Forma general
Con +s se indica la fecha inicial y con +e la fecha final donde se desea hacer la revisión.
```bash
python revision.py +s 20210601T000000 +e 20210620T000000
```
### Filtrar usuario
Se añade +u para poner el usuario.
```bash
python revision.py +s 20210601T000000 +e 20210620T000000 +u ecastillo
```

### Guardar en un archivo
Se añade +o (True o False) donde True es usado para guardar. Se debe añadir "> archivo.txt"
```bash
python revision.py +s 20210601T000000 +e 20210620T000000 +u ecastillo +o True > archivo.txt
```


## Autor

- Angel Agudelo adagudelo@sgc.gov.co
- Emmanuel  Castillo ecastillo@sgc.gov.co
- Laura Mercado lmercado@sgc.gov.co


