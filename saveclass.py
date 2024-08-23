import csv
import re
import hashlib
from typing import Generator, Any
from math import isnan
from pathlib import Path

# pequeño programa que permite crear, borrar y leer entradas hacia un archivo csv a
# partir de los valores de una clase
class CsvClassSave:
    """clase CsvClassSave una clase para guardar los atributos de instancia de
    distintas clases en archivos csv
    Argumentos de iniciacion: 
    - path_file la ruta al archivo csv donde se van a guardar
    los atributos de clase
    - object objeto del cual se van a guardar los atributos, si el objeto no soporta
     el metodo __dict__ no sera posible escribir entradas al csv
    -single atributo que determina que comportamiento tomara el programa si es True
    los atributos del objeto seran el encabezado del csv y solo se podran guardar 
    idealmente objetos de un solo tipo o que tenga la misma cantidad de atributos
    y nombres que el primer objeto guardado en el csv, si es False se podran guardar
    objetos de distintas clases y el encabezado solo permitira 3 valores para cada una
    un indice, un nombre de clase y los atributos de cada una
    - col_sep: el separador usado para las columnas del archivo csv defult es '|'
    - header: los encabezados para las columnas del archivo, debe ser tupla con 3 strings
     default es ("INDICE", "CLASE", "ATRIBUTOS") 
    - exclude: debe ser una tupla de str con los nombres de los atributos que no se quieran
     guardar del objeto, esto para evitar guardar atributos que sean de funcionamiento interno
     (no de iniciacion) que no aporten información importante, el default es None e indica que
     todos los atributos seran guardados, lo atributos a excluir debe ser pasado en una tupla
     que contenga solo str, para negar la tupla o en otras palabras que enves de que la misma 
     se ejecute incluyendo solo los atributos en ella se debe pasar como primer elemento
     de la misma el str '!'"""
    # archivo backup de la ruta del csv pasado a esta clase, sirve de respaldo
    # de la misma y tambien para habilitar la opcion de eliminar entradas del csv
    # os.path.dirname(os.path.abspath(__file__)) = str(Path(__file__).parent)
    backup_single = Path(fr"{Path(__file__).parent}\backup\backupsingle.csv")
    backup_multi = Path(fr"{Path(__file__).parent}\backup\backupmulti.csv")
    # limite maximo de filas por archivo csv es más para no terminar con un programa
    # muy lento por la gran cantidad de filas y lo poco optimizado que es la busqueda y borrado
    # de esntradas en un csv en comparación con algo como una base de datos
    max_row_limit: int = 10_000

    # since we only save a class or object info and everyting
    # in python is an onject we use type any for object
    def __init__(self, path_file: str, object: Any = None,  single: bool = False, col_sep: str = "|", 
                 header: tuple[str, str, str] = ("INDICE", "CLASE", "ATRIBUTOS"), 
                 exclude: None | tuple = None) -> None:
        self.file_path = path_file
        self.object = object
        self.single = single
        self.delimiter = col_sep
        self.header = header
        self.exclude = exclude
        self.can_save = True
        if "__dict__" not in dir(self.object):
            self.can_save = False
        self.accepted_files: tuple[str, str] = (self.file_path, str(self.backup_multi if not self.single else self.backup_single))
        # crea el directorio del backup si no exite
        try:
            # this is equal to os.makedirs(os.path.dirname(str(self.backup)))
            Path(self.backup_single.parent).mkdir()
        except FileExistsError:
            pass
        else:
            with open(str(self.backup_multi), "w", newline="", encoding="utf-8") as _:
                pass
            with open(str(self.backup_single), "w", newline="", encoding="utf-8") as _:
                pass
        # se verifica que el backup y el archivo principal tengan el mismo contenido
        # a la hora de ejecutar el programa mediante la comparacion de un hash de
        # ambos archivos y son distintos el backup sobre escribe al archivo principal
        hash_list: list[str] = []
        for csv_file in self.accepted_files:
            # all of this can be done way cleaner
            # using file_digest from haslib
            # but is only supported on newer python versions
            # hash algorithm
            hash_: hashlib._Hash  = hashlib.sha256()
            # buffer size
            buffer_size  = bytearray(128*1024)
            # creates a binary view from a file of size buffer_size
            buffer_view = memoryview(buffer_size)
            with open(csv_file, 'rb', buffering=0) as file_hash:
                for chunk_ in iter(lambda : file_hash.readinto(buffer_view), 0):
                    hash_.update(buffer_view[:chunk_])
            hash_hex: str = hash_.hexdigest()
            if hash_hex not in hash_list:
                hash_list.append(hash_hex)

        # we give priority to the backup the user
        # shouldn't change the file manually
        self.current_rows: int = self.__len__()
        if len(hash_list) == 2:
            self.borrar_datos(delete_index="borrar todo", rewrite=True)
        # si el archivo tiene más filas que el limite fijado
        # entonces el usuario no puede escribir nuevas entradas
        if self.current_rows > self.max_row_limit:
            self.can_save = False
        # our backup must be wipe out and rebuild every time the 
        # program runs to support writing to more than one fixed
        # csv file
        if self.single:
            if self.current_rows:
                self.new_head = tuple((val for val in next(self.leer_datos_csv(back_up=True))))


    @property
    def file_path(self) -> str:
        return self._file_path
    
    # setter para la ruta del archivo csv 
    # el usuario debe introducir un str
    # que tambien sea una ruta existente
    # en el sistema operativo
    @file_path.setter
    def file_path(self, value) -> None:
        if isinstance(value, str):
            my_path: Path = Path(value)
            # is file check if the file exist in the os
            if my_path.suffix.lower() == ".csv":
                if my_path.is_file():
                    self._file_path = value
                else:
                    raise ValueError(f"la ruta introducida ({value})\nno existe por favor "
                                     "cree el archivo en una locación valida")
            else:
                raise ValueError("la ruta del archivo debe ser de extension .csv "
                                 f"y usted entro uno de extension {my_path.suffix}")
        else:
            raise ValueError(f"el valor introducido debe ser str, pero fue {type(value).__name__}")
        
    @property
    def single(self) -> bool:
        return self._single
    
    @single.setter
    def single(self, value) -> None:
         if isinstance(value, bool):
             self._single = value
         else:
            raise ValueError(f"el valor debe ser bool pero fue {type(value).__name__}")
        
    @property
    def delimiter(self) -> str:
        return self._delimiter
    
    # setter para el separador del csv
    # se verifica que sea un str cuyo
    # tamaño no supero un caracter
    @delimiter.setter
    def delimiter(self, value) -> None:
        if isinstance(value, str):
            if len(value) == 1:
                self._delimiter = value
            else:
                raise ValueError(f"su valor {value} debe contener solo un caracter")
        else:
            raise ValueError(f"el valor debe ser str pero fue {type(value).__name__}")

    @property
    def header(self) -> tuple[str, str, str]:
        return tuple((str(val).upper() for val in self._header))
    
    # setter para el encabezado del csv
    # debe ser una tupla con tres str
    @header.setter
    def header(self, value) -> None:
        match value:
            # this is how you do type pattern matching
            case tuple((str(), str(), str())):
                self._header = value
            case _:
                raise ValueError(f"el valor debe ser una tupla con 3 str pero este fue {type(value).__name__}: {value}")
            
    @property
    def exclude(self) -> None | tuple[str]:
        return self._exclude
    
    # setter para la tupla de atributos
    # que deben ser excluidos o incluidos en el csv
    # el valor debe ser una tupla con almenos un elemento
    # y los elementos deben ser todos del tipo str
    @exclude.setter
    def exclude(self, value) -> None:
        if value is None:
            self._exclude = value
        elif isinstance(value, tuple):
            if not value:
                raise ValueError(f"el valor debe contener al menos un str pero tubo {len(value)}")
            if all([isinstance(item, str) for item in value]):
                self._exclude = value
            else:
                raise ValueError(f"todos los valores de la tupla deben ser str y su tupla tubo {', '.join([str(type(item).__name__) for item in value])}")
        else:
            raise ValueError(f"el valor debe ser una tupla o None pero fue {type(value).__name__}")

    # a generator can have 3 types of value the type of the
    # yield value (first value), the type of the return value (third value, an exhausted iterator 
    # raises an StopIteration error and the value of it is the value of the return)
    # and the second value is the type of the value an iterator can accept from outside using
    # the .send() method of a generator
    def leer_datos_csv(self, search: str = "", back_up: bool = False, escaped: bool = False) -> Generator[list[str], None, None]:
        """metodo publico leer datos csv
        Argumentos: 
        -search es la str que se usa para buscar dentro del csv 
        -back_up es para buscar en el csv de backup enves del original
        -escaped es para saber si se debe escapar algun caracter especial
        en el search str cuando se realize la busqueda mediante expresiones regulares
        valor de retorno: un generador que permite enviar de una en una las lineas dentro del archivo csv
        Para saber que sintaxis es ocupada para buscar por indice refierace al metodo estatico publico 
        return_pattern"""
        if self.current_rows > 0:
            with open(str(self.file_path) if not back_up else (str(self.backup_multi) if not self.single else str(self.backup_single)), 
                      "r", newline="", encoding="utf-8") as csv_reader:
                read = csv.reader(csv_reader, delimiter=self.delimiter)
                # usando generadores para evitar cargar todo el archivo a memoria
                if search:
                    yield next(read)
                    if isinstance (to_search:= self.return_patern(search), tuple):
                        operation: str | None = to_search[0]
                        vals_to_search: list = [num for num in to_search[-1] if self.current_rows >= num >= 0]
                        if not vals_to_search:
                            operation = "-"
                            vals_to_search = [0,]
                        if operation == ":":
                            if len(vals_to_search) == 1:
                                vals_to_search.append(self.current_rows)
                            # mypy is a pain in the ass
                            vals_to_search.append(float("nan"))
                        mark: float | int = vals_to_search[-1]
                        for count, row in enumerate(read, 1):
                            if isnan(mark):
                                low_lim: int = vals_to_search[0]
                                up_lim: int = vals_to_search[1]
                                if low_lim <= count <= up_lim:
                                    yield row
                                elif count > up_lim:
                                    break
                            else:
                                if count in vals_to_search:
                                    yield row
                                # return pattern sort the values so we are sure
                                # that mark is the biggest one
                                elif count > mark:
                                    break
                    else:
                        if self.single:
                            # this regex is sus
                            list_of_match = re.findall(r"[^=\s~]+=[^=\s~]+", search)
                            if list_of_match:
                                is_in_head = tuple((str(item).split("=") for item in list_of_match ))
                                col_to_look = tuple(((self.new_head.index(str(val[0]).upper()), val[1]) for val in is_in_head if str(val[0]).upper() in self.new_head))
                                for row in read:
                                    if "~" in search:
                                        if all(map(lambda val: row[val[0]] == val[1], col_to_look)):
                                            yield row
                                    else:
                                        if list(filter(None, map(lambda val: row[val[0]] == val[1], col_to_look))):
                                            yield row
                        for row in read:
                            if row and re.search(f"^.*{re.escape(search) if not escaped else search}.*$", "".join(row[1:]), re.IGNORECASE) is not None:
                                yield row
                else:
                    for row in read:
                        if row:
                            yield row
        
    def guardar_datos_csv(self, enforce_unique = None) -> str:
        """metodo publico guardar datos csv
        permite escribir una nueva entrada en un archivo csv y retornar la nueva entrada añadida
        Argumentos: 
        -enforce unique puede ser None o una tupla con str, permite decidir si 
        el valor del o los atributos de la clase debe ser unico con respecto a los presentes en el csv, 
        si es tupla debe ser de la siguiente forma ('nombre_atributo',) para un atributo y 
        ('nombre_atributo1', 'nombre_atributo2', 'nombre_atributo3') para multiples ideal si se guardan 
        atributos de solo una clase o un conjunto de clases con un padre y atributos en comun, si su 
        valor es None no se chequea que el atributo deba ser unico
       Valor de retorno: un str de la nueva entrada creada y si se especifico enforce unique y se encontro
       que la entrada a guardar ya estaba presente se retorna el str presente
        """
        if not self.can_save:
            return ("\nAdvertencia: Su entrada no fue creada ya que para mantener la eficiencia de este programa "
                    f"recomendamos\nlimtar el numero de entrada a {self.max_row_limit - 3_000} "
                    f"favor de ir a\n{self.file_path}\nhacer una, copia reiniciar el programa y\n"
                    "borrar todas las entradas para proseguir normalmente\nde aqui en adelante "
                    "solo se aceptaran operaciones de lectura y borrado de entradas solamente")

        if enforce_unique is not None and self.current_rows > 1:
            if not isinstance(enforce_unique, tuple):
                raise ValueError(f"el parametro enforce_unique debe ser una tupla pero fue {type(enforce_unique).__name__}")
            elif not enforce_unique:
                raise ValueError(f"la tupla debe contener al menos un str")
            elif not all([isinstance(item, str) for item in enforce_unique]):
                raise ValueError(f"la tupla solo debe contener str su tupla contiene {', '.join([str(type(item).__name__) for item in enforce_unique])}")
            # strip("_") para eliminar el _ que es puesto cuando 
            # se tiene atributos que usan algun tipo decorador como
            # el property y setter
            # asi puedo buscar en atributos no consecutivos para ver si son unicos
            if not self.single:
                vals_to_check: str = ".+".join([re.escape(f"{str(key).strip('_')}: {val}") for 
                                                key, val in self.object.__dict__.items() if str(key).strip("_") in enforce_unique])
                for entry in self.leer_datos_csv(search=vals_to_check, back_up=True, escaped=True):
                    # esto por si el csv contiene elementos de distintas clases
                    if entry[1] == str(self.object.__class__):
                        return "presente"
            else:
                vals_to_check = " ".join([f"{str(key).strip('_')}={val}" for 
                                          key, val in self.object.__dict__.items() if str(key).strip("_") in enforce_unique])
                skip_first = self.leer_datos_csv(search=vals_to_check, back_up=True)
                next(skip_first)
                for entry in skip_first:
                    return "presente"
        if not self.current_rows:
            for files in self.accepted_files:
                with open(files, "a", newline="", encoding="utf-8") as csv_writer:
                    write = csv.writer(csv_writer, delimiter=self.delimiter)
                    if not self.single:
                        write.writerow(self.header)
                    else:
                        if self.exclude is not None:
                            if self.exclude[0] == "!":
                                self.new_head = [self.header[0], *[str(key).strip("_").upper() for key in self.object.__dict__ if str(key).strip("_") in self.exclude]]
                                write.writerow(self.new_head)
                            else:
                                self.new_head = [self.header[0], *[str(key).strip("_").upper() for key in self.object.__dict__ if str(key).strip("_") not in self.exclude]]
                                write.writerow(self.new_head)
                        else:
                            self.new_head = [self.header[0], *[str(key).strip("_").upper() for key in self.object.__dict__]]
                            write.writerow(self.new_head)
            self.current_rows += 1
        if self.exclude is not None:
            if self.exclude[0] == "!":
                if not self.single:
                    class_repr: str = ", ".join([f"{str(key).strip('_')}: {val}" for key, val in self.object.__dict__.items() if str(key).strip("_") in self.exclude])
                else:
                    class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.object.__dict__.items() if str(key).strip("_") in self.exclude]
                    if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
                        raise ValueError("en modo single = True solo se permiten objetos "
                                         f"con el mismo número de atributos y nombres que el actual {', '.join(self.new_head)}")
            else:
                if not self.single:
                    class_repr = ", ".join([f"{str(key).strip('_')}: {val}" for key, val in self.object.__dict__.items() if str(key).strip("_") not in self.exclude])
                else:
                    class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.object.__dict__.items() if str(key).strip("_") not in self.exclude]
                    if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
                        raise ValueError("en modo single = True solo se permiten objetos "
                                         f"con el mismo número de atributos y nombres que el actual {', '.join(self.new_head)}")
        else:
            if not self.single:
                class_repr = ", ".join([f"{str(key).strip('_')}: {val}" for key, val in self.object.__dict__.items()])
            else:
                class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.object.__dict__.items()]
                if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
                        raise ValueError("en modo single = True solo se permiten objetos "
                                         f"con el mismo número de atributos y nombres que el actual {', '.join(self.new_head)}")
        for count, files in enumerate(self.accepted_files):
            with open(files, "a", newline="", encoding="utf-8") as csv_writer:
                write = csv.writer(csv_writer, delimiter=self.delimiter)
                if not count:
                    self.current_rows += 1
                if not self.single:
                    write.writerow([f"[{self.current_rows-1}]", self.object.__class__, class_repr])
                else:
                    write.writerow([f"[{self.current_rows-1}]", *[val[1] for val in class_repr]])
        if not self.single:
            return f"\n{f'{self.delimiter}'.join(self.header)}\n[{self.current_rows-1}]{self.delimiter}{self.object.__class__}{self.delimiter}{class_repr}"
        else:
            return f"\n{f'{self.delimiter}'.join([*self.new_head])}\n[{self.current_rows-1}]{self.delimiter}{f'{self.delimiter}'.join([val[1] for val in class_repr])}"

    def borrar_datos(self, delete_index: str = "", rewrite: bool = False) -> str | list:
        if delete_index == "borrar todo":
            if rewrite:
                with open(self.file_path, "w", newline="", encoding="utf-8") as _:
                    pass
                with open(self.file_path, "w", newline="", encoding="utf-8") as write_filter:
                    filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                    for entry in self.leer_datos_csv(back_up=True):
                        filter_.writerow(entry)
                return "rewrite"
            if self.current_rows <= 1:
                return "nada"
            for files in self.accepted_files:
                with open(files, "w", newline="", encoding="utf-8") as _:
                    pass
            self.current_rows = 0
            return "todo"
        else:
            if self.current_rows <= 1:
                return "nada"
            # use regex to accpet multiple entries to delete
            if isinstance(to_delete:= self.return_patern(delete_index), tuple):
                # to get rid of things like 00 or 03, 056
                operation: str | None = to_delete[0]
                vars_to_delete: list = [num for num in to_delete[-1] if self.current_rows >= num >= 0]
                if not vars_to_delete:
                    raise ValueError("ninguno de los valores ingresados corresponde al indice de alguna entrada")
                if operation == ":":
                    if len(vars_to_delete) == 1:
                        vars_to_delete.append(self.current_rows)
                    vars_to_delete.append("range")
                else:
                    vars_to_delete = [f"[{num}]" for num in vars_to_delete]
            else:
                raise ValueError("utilize uno de los siguientes formatos para borrar una entrada:\n"
                                    "[n], [n:m], [n:], [n-m-p] (hasta 10) remplazando las letras por el indice "
                                    "de lo que desee eliminar ")
            # copying all the data except entries to delete from backup file to main file
            deleted_: list[str] = [f"{self.delimiter}".join(self.header if not self.single else [self.header[0], *self.new_head]),]
            with open(self.file_path, "w", newline="", encoding="utf-8") as write_filter:
                filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                count: int = 1
                mark: str = vars_to_delete[-1]
                row_generator: Generator[list[str], None, None] = self.leer_datos_csv(back_up=True)
                # to not count header
                filter_.writerow(next(row_generator))
                for entry in row_generator:
                    if mark == "range":
                        low_lim: int = vars_to_delete[0]
                        up_lim: int = vars_to_delete[1]
                        current = int(re.sub(r"[\[\]]", "", entry[0]))
                        if current < low_lim or current > up_lim:
                            entry[0] = f"[{count}]"
                            filter_.writerow(entry)
                            count += 1
                        else:
                            deleted_.append(f"{self.delimiter}".join(val for val in entry))

                    else:
                        if entry[0] not in vars_to_delete:
                            entry[0] = f"[{count}]"
                            filter_.writerow(entry)
                            count += 1
                        else:
                            deleted_.append(f"{self.delimiter}".join(val for val in entry))
            # deleting all data on backup (is not up to date)
            # syncronizing backup
            # should I use asyncio?
            with open(str(self.backup_multi if not self.single else self.backup_single), 
                      "w", newline="", encoding="utf-8") as write_filter:
                filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                for entry in self.leer_datos_csv():
                    filter_.writerow(entry)
            self.current_rows = self.__len__()
            return deleted_

    #static method
    @staticmethod
    def return_patern(str_pattern) -> tuple | None:
        """ metodo estatico publico return patern
        Argumento: 
        -str pattern el str en el cual se buscara el patron deseado
        este metodo especificamente sirve para saber si el usuario introduce el patron
        correcto para buscar o eliminar alguna entrada del csv, en nuestro caso el usuario
        debe ingresar ya sea [numero], [numero:], [numero1:numero2] o [numero1-numero2-numero3]
        -Valor de retorno: una tupla donde el primer valor es el tipo de operacion, ya sea
        ':' (por rango), '-' (más de un valor especifico) o None (solo un valor especifico) y el
        segundo valor es una lista con los valores a eliminar. El valor de retorno es None si
        no se encontro el patron en el str pattern
        """
        if not isinstance(str_pattern, str):
            raise ValueError(f"debe ingresar un str donde buscar un patron, pero se recibion {type(str_pattern).__name__}")
        regex_obj = re.search(r"^\[(?:\d+(:)\d*|(?:\d+(-)){0,9}\d+)\]$", str_pattern)
        if regex_obj is not None:
            separator: list = [sep for sep in regex_obj.groups() if sep is not None]
            str_pattern = re.sub(r"[\[\]]", "", str_pattern)
            if separator:
                pattern_nums: list[int] = [int(val) for val in str_pattern.split(separator[0]) if val]
                pattern_nums.sort()
                return separator[0], pattern_nums
            return None, [int(str_pattern),]
        return None

    def __len__(self) -> int:
        with open(str(self.backup_multi if not self.single else self.backup_single), 
                  "r", newline="", encoding="utf-8") as csv_reader:
            read = csv.reader(csv_reader, delimiter=self.delimiter)
            # clever method to get the len and avoid putting
            # all the contents of the file in memory
            # this 1 for _ in read is an iterator
            return sum(1 for _ in read)

