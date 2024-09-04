import csv
import re
import hashlib
from typing import Generator, Any
from math import isnan
from pathlib import Path
from datetime import date


# pequeño programa que permite crear, borrar y leer entradas hacia un archivo csv a
# partir de los valores de una clase
# modo single = True caracteres reservados | y & para búsquedas, en modo single = False : para exportar
class CsvClassSave:
    """clase CsvClassSave una clase para guardar los atributos de instancia de
    distintas clases en archivos csv

    Argumentos de iniciación:

    - path_file la ruta al archivo csv donde se van a guardar
    los atributos de clase
    - class_object objeto del cual se van a guardar los atributos, si el objeto no soporta
    el método __dict__ no sera posible escribir entradas al csv
    - single atributo que determina que comportamiento tomara el programa si es True
    los atributos del objeto serán el encabezado del csv y solo se podrán guardar
    idealmente objetos de un solo tipo o que tenga la misma cantidad de atributos
    y nombres que el primer objeto guardado en el csv, si es False se podrán guardar
    objetos de distintas clases y el encabezado solo permitirá 3 valores para cada una
    un indice, un nombre de clase y los atributos de cada una
    - col_sep: el separador usado para las columnas del archivo csv default es '|'
    - header: los encabezados para las columnas del archivo, debe ser tuple con 3 strings
    default es ("INDICE", "CLASE", "ATRIBUTOS")
    - exclude: debe ser una tuple de str con los nombres de los atributos que no se quieran
    guardar del objeto, esto para evitar guardar atributos que sean de funcionamiento interno
    (no de iniciación) que no aporten información importante, el default es None e indica que
    todos los atributos serán guardados, lo atributos a excluir debe ser pasado en una tuple
    que contenga solo str, para negar la tuple o en otras palabras que envés de que la misma
    se ejecute incluyendo solo los atributos en ella se debe pasar como primer elemento
    de la misma el str '!'
    - check_hash: da la opción para desactivar ella creación y comparación de hashes para determinar
    si el archivo esta sincronizado con el backup, su default es True, el comportamiento puede ser
    inesperado si se decide poner como False y algún archivo sufre modificaciones
    """
    # archivo backup de la ruta del csv pasado a esta clase, sirve de respaldo
    # de la misma y también para habilitar la opción de eliminar entradas del csv
    # os.path.dirname(os.path.abspath(__file__)) = str(Path(__file__).parent)
    backup_single = Path(fr"{Path(__file__).parent}\backup\backup_single.csv")
    backup_multi = Path(fr"{Path(__file__).parent}\backup\backup_multi.csv")
    # limite máximo de filas por archivo csv es más para no terminar con un programa
    # muy lento por la gran cantidad de filas y lo poco optimizado que es la búsqueda y borrado
    # de entradas en un csv en comparación con algo como una base de datos
    max_row_limit: int = 20_000

    # since we only save a class or object info and everything
    # in python is an object we use type any for object
    def __init__(self, path_file: str, class_object: Any = None, single: bool = False, col_sep: str = "|",
                 header: tuple[str, str, str] = ("INDICE", "CLASE", "ATRIBUTOS"), exclude: None | tuple = None,
                 check_hash: bool = True) -> None:
        self.file_path = path_file
        self.object = class_object
        self.single = single
        self.delimiter = col_sep
        self.header = header
        self.exclude = exclude
        self.can_save = True
        self.hashing = check_hash
        if "__dict__" not in dir(self.object):
            self.can_save = False
        self.accepted_files: tuple[str, str] = (self.file_path,
                                                str(self.backup_multi if not self.single else self.backup_single))
        # crea el directorio del backup si no existe
        backup_directory = Path(self.backup_single.parent)
        if not backup_directory.is_dir():
            backup_directory.mkdir()
        if self.single and not self.backup_single.is_file():
            with open(str(self.backup_single), "w", newline="", encoding="utf-8") as _:
                pass
        elif not self.single and not self.backup_multi.is_file():
            with open(str(self.backup_multi), "w", newline="", encoding="utf-8") as _:
                pass
        # se verifica que el backup y el archivo principal tengan el mismo contenido
        # a la hora de ejecutar el programa mediante la comparación de un hash de
        # ambos archivos y son distintos el backup sobre escribe al archivo principal
        if self.hashing:
            hash_list: list[str] = []
            for csv_file in self.accepted_files:
                # all of this can be done way cleaner
                # using file_digest from hashlib
                # but is only supported on newer python versions
                # hash algorithm
                hash_ = hashlib.sha256()
                # buffer size
                buffer_size = bytearray(128 * 1024)
                # creates a binary view from a file of size buffer_size
                buffer_view = memoryview(buffer_size)
                with open(csv_file, 'rb', buffering=0) as file_hash:
                    for chunk_ in iter(lambda: file_hash.readinto(buffer_view), 0):
                        hash_.update(buffer_view[:chunk_])
                hash_hex: str = hash_.hexdigest()
                if hash_hex not in hash_list:
                    hash_list.append(hash_hex)

            # we give priority to the backup the user
            # shouldn't change the file manually
            if not self.single:
                self.current_classes: list = []
            self.current_rows: int = self.__len__()
            if len(hash_list) == 2:
                # like this for the code to execute
                next(self.borrar_datos(delete_index="borrar todo", rewrite=True))
        # si el archivo tiene más filas que el limite fijado
        # entonces el usuario no puede escribir nuevas entradas
        # our backup must be wipe out and rebuild every time the
        # program runs to support writing to more than one fixed
        # csv file
        else:
            if not self.single:
                self.current_classes: list = []
            self.current_rows = self.__len__()
        if self.single:
            if self.current_rows:
                self.new_head = tuple((val.upper() for val in next(self.leer_datos_csv(back_up=True))))

    @property
    def file_path(self) -> str:
        return self._file_path

    # setter para la ruta del archivo csv
    # el usuario debe introducir un str
    # que también sea una ruta existente
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
    # tamaño no supere un carácter
    @delimiter.setter
    def delimiter(self, value) -> None:
        if isinstance(value, str):
            if len(value) == 1:
                self._delimiter = value
            else:
                raise ValueError(f"su valor {value} debe contener solo un carácter")
        else:
            raise ValueError(f"el valor debe ser str pero fue {type(value).__name__}")

    @property
    def header(self) -> tuple[str]:
        return tuple((str(val).upper() for val in self._header))

    # setter para el encabezado del csv
    # debe ser una tuple con tres str
    @header.setter
    def header(self, value) -> None:
        match value:
            # this is how you do type pattern matching
            case tuple((str(), str(), str())):
                self._header = value
            case _:
                raise ValueError(f"el valor debe ser una tuple con 3 str pero este fue {type(value).__name__}: {value}")

    @property
    def exclude(self) -> None | tuple[str]:
        return self._exclude

    # setter para la tuple de atributos
    # que deben ser excluidos o incluidos en el csv
    # el valor debe ser una tuple con al menos un elemento
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
                raise ValueError(
                    "todos los valores de la tuple "
                    f"deben ser str y su tuple tubo {', '.join([str(type(item).__name__) for item in value])}")
        else:
            raise ValueError(f"el valor debe ser una tuple o None pero fue {type(value).__name__}")

    @property
    def hashing(self) -> bool:
        return self._hashing

    @hashing.setter
    def hashing(self, value) -> None:
        if isinstance(value, bool):
            self._hashing = value
        else:
            raise ValueError(f"el valor debe ser bool pero fue {type(value).__name__}")

    # a generator can have 3 types of value the type of the
    # yield value (first value), the type of the return value (third value, an exhausted iterator
    # raises an StopIteration error and the value of it is the value of the return)
    # and the second value is the type of the value an iterator can accept from outside using
    # the .send() method of a generator
    def leer_datos_csv(self, search="", back_up=False, escaped=False) -> Generator[list[str], None, str]:
        """método publico leer_datos_csv

        Argumentos:

        - search es la str que se usa para buscar dentro del csv
        - back_up es para buscar en el csv de backup envés del original
        - escaped es para saber si se debe escapar algún carácter especial
        en el search str cuando se realize la búsqueda mediante expresiones regulares

        Valor de retorno:

        - un generador que permite enviar de una en una las lineas dentro del archivo csv
        Para saber que sintaxis es ocupada para buscar por indice refiérase al método estático publico
        return_pattern

        Excepciones:

        - ValueError si los tipos de los argumentos no son los apropiados
        """
        if not isinstance(search, str):
            raise ValueError(f"el argumento search debe ser un str pero fue {type(search).__name__}")
        for name, item in {"back_up": back_up, "escaped": escaped}.items():
            if not isinstance(item, bool):
                raise ValueError(f"el argumento {name} debe ser un bool pero fue {type(item).__name__}")
        if self.current_rows > 0:
            with open(str(self.file_path) if not back_up
                      else (str(self.backup_multi) if not self.single else str(self.backup_single)),
                      "r", newline="", encoding="utf-8") as csv_reader:
                read = csv.reader(csv_reader, delimiter=self.delimiter)
                # usando generadores para evitar cargar todo el archivo a memoria
                if search:
                    if isinstance(to_search := self.return_pattern(search), tuple):
                        yield next(read)
                        operation: str | None = to_search[0]
                        vals_to_search: list = [num for num in to_search[-1] if self.current_rows >= num >= 0]
                        if not vals_to_search:
                            operation = "-"
                            vals_to_search = [0, ]
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
                            list_of_match: list = self.__query_parser(search)
                            if list_of_match:
                                except_col = ()
                                if isinstance(list_of_match[0], tuple):
                                    except_col = except_col + list_of_match.pop(0)
                                    header = next(read)
                                    yield [header[0]] + [header[val] for val in range(1, len(header)) if
                                                         val not in except_col]
                                else:
                                    yield next(read)
                                function_match: list = []
                                if list_of_match and isinstance(list_of_match[-1], str):
                                    operand, column, *_ = str(list_of_match.pop()).split(":")
                                    if operand == "COUNT":
                                        function_match += ["COUNT", 0]
                                    elif operand == "LIMIT":
                                        try:
                                            limit_result = int(column)
                                        except ValueError:
                                            pass
                                        else:
                                            if limit_result > 0:
                                                function_match += ["LIMIT", limit_result + 1]
                                    elif column and column.upper() in self.new_head:
                                        col_index = self.new_head.index(str(column).upper())
                                        if not except_col or col_index not in except_col:
                                            if operand == "AVG":
                                                function_match += [operand, 0, 0, col_index]
                                            elif operand == "MIN":
                                                function_match += [operand, [float("inf"), None], col_index]
                                            elif operand == "MAX":
                                                function_match += [operand, [float("-inf"), None], col_index]
                                            elif operand == "SUM":
                                                function_match += [operand, 0, col_index]
                                            elif operand == "UNIQUE":
                                                function_match += [operand, set(), col_index]
                                            elif operand == "ASC" or operand == "DESC":
                                                header_offset = sum((1 for item in except_col if item < col_index))
                                                function_match += [operand, [], col_index - header_offset, col_index]
                                for row in read:
                                    bool_values: list = []
                                    for element in list_of_match:
                                        if isinstance(element, list):
                                            try:
                                                head_index = self.new_head.index(element[0])
                                            except IndexError:
                                                return "error de sintaxis"
                                            # so you can search from index to
                                            if head_index > 0:
                                                val = row[head_index]
                                            else:
                                                val = re.sub(r"[\[\]]", "", row[head_index])
                                            try:
                                                row_val, expected_val = float(val), float(element[-1])
                                            except ValueError:
                                                pass
                                            else:
                                                bool_values.append(element[1](row_val, expected_val))
                                                continue
                                            try:
                                                # the only standard I will support
                                                row_time = date.fromisoformat(val) 
                                                expected_time = date.fromisoformat(element[-1])
                                            except ValueError:
                                                pass
                                            else:
                                                bool_values.append(element[1](row_time, expected_time))
                                                continue
                                            try:
                                                bool_values.append(element[1](val, element[-1]))
                                            except TypeError:
                                                bool_values.append(False)
                                        else:
                                            bool_values.append(element)
                                    if not bool_values:
                                        yield "error de sintaxis"
                                        return "sintaxis no valida búsqueda terminada"
                                    else:
                                        current_value = bool_values[0]
                                        if len(bool_values) != 1:
                                            operation = "?"
                                            for item in bool_values:
                                                if item == "|":
                                                    operation = "|"
                                                elif item == "&":
                                                    operation = "&"
                                                else:
                                                    if operation == "|":
                                                        current_value = current_value or item
                                                    elif operation == "&":
                                                        current_value = current_value and item
                                        if current_value:
                                            if function_match:
                                                if function_match[0] == "LIMIT":
                                                    function_match[-1] -= 1
                                                    if function_match[-1] == 0:
                                                        return ("se alcanzo el limite de entradas "
                                                                f"requeridas LIMIT:{function_match[-1]}")
                                                elif function_match[0] == "COUNT":
                                                    function_match[-1] += 1
                                                elif function_match[0] in ("UNIQUE", "PRESENT"):
                                                    before_len = len(function_match[1])
                                                    function_match[1].add(row[function_match[-1]])
                                                    if before_len != len(function_match[1]):
                                                        function_match[0] = "UNIQUE"
                                                    else:
                                                        function_match[0] = "PRESENT"
                                                elif function_match[0] in ("ASC", "DESC"):
                                                    if except_col:
                                                        function_match[1].append(
                                                            [row[0]] + [row[item] for item in
                                                                        range(1, len(row)) if
                                                                        item not in except_col])
                                                    else:
                                                        function_match[1].append(row)
                                                else:
                                                    function_val = row[function_match[-1]]
                                                    for is_type in (float, date.fromisoformat, str):
                                                        try:
                                                            val_type = is_type(function_val)
                                                        except ValueError:
                                                            pass
                                                        else:
                                                            if (function_match[0] == "AVG" and not 
                                                                    isnan(function_match[1])):
                                                                try:
                                                                    function_match[1] += val_type
                                                                    function_match[2] += 1
                                                                except TypeError:
                                                                    function_match[1] = float("nan")
                                                                    function_match[2] = 0
                                                            elif function_match[0] == "MAX":
                                                                if (function_match[1][0] == float("-inf") 
                                                                        and isinstance(val_type, date)):
                                                                    function_match[1][0] = val_type
                                                                else:
                                                                    if function_match[1][0] != "STR":
                                                                        try:
                                                                            if val_type > function_match[1][0]:
                                                                                function_match[1][0] = val_type
                                                                        except TypeError:
                                                                            function_match[1][0] = "STR"
                                                                        if function_match[1][1] is not None:
                                                                            if function_val > function_match[1][1]:
                                                                                function_match[1][1] = function_val
                                                                        else:
                                                                            function_match[1][1] = function_val
                                                                    else:
                                                                        if function_val > function_match[1][1]:
                                                                            function_match[1][1] = function_val
                                                            elif function_match[0] == "MIN":
                                                                if (function_match[1][0] == float("inf") 
                                                                        and isinstance(val_type, date)):
                                                                    function_match[1][0] = val_type
                                                                else:
                                                                    if function_match[1][0] != "STR":
                                                                        try:
                                                                            if val_type < function_match[1][0]:
                                                                                function_match[1][0] = val_type
                                                                        except TypeError:
                                                                            function_match[1][0] = "STR"
                                                                        if function_match[1][1] is not None:
                                                                            if function_val < function_match[1][1]:
                                                                                function_match[1][1] = function_val
                                                                        else:
                                                                            function_match[1][1] = function_val
                                                                    else:
                                                                        if function_val < function_match[1][1]:
                                                                            function_match[1][1] = function_val
                                                            elif (function_match[0] == "SUM" and not 
                                                                    isnan(function_match[1])): 
                                                                try:
                                                                    function_match[1] += val_type
                                                                except TypeError:
                                                                    function_match[1] = float("nan")
                                                            break
                                                if function_match[0] not in ("LIMIT", "UNIQUE"):
                                                    continue
                                            if except_col:
                                                yield [row[0]] + [row[item] for item in range(1, len(row)) if
                                                                  item not in except_col]
                                            else:
                                                yield row
                                if function_match:
                                    if len(function_match) == 4:
                                        if function_match[0] not in ("ASC", "DESC"):
                                            yield ["AVG", self.new_head[function_match[-1]],
                                                   function_match[1] / function_match[2] if function_match[2] else 0]
                                        else:
                                            for types_comp in (float, date.fromisoformat, str):
                                                try:
                                                    function_match[1].sort(
                                                        key=lambda x: types_comp(x[function_match[2]]),
                                                        reverse=True if function_match[0] == "DESC" else False)
                                                except ValueError:
                                                    pass
                                            for sorted_item in function_match[1]:
                                                yield sorted_item
                                    elif len(function_match) == 3:
                                        if function_match[0] not in ("UNIQUE", "PRESENT"):
                                            if function_match[0] not in ("MIN", "MAX"):
                                                yield [function_match[0], self.new_head[function_match[-1]],
                                                       function_match[1] if not isnan(function_match[1]) else 0]
                                            else:
                                                yield [function_match[0], self.new_head[function_match[-1]],
                                                       function_match[1][0] if function_match[1][0] != "STR" 
                                                       else function_match[1][1]]
                                        else:
                                            yield ["UNIQUE", self.new_head[function_match[-1]], len(function_match[1])]
                                    # LIMIT might be able to get to here in is set
                                    # bigger than the total amount of entries on a search
                                    elif function_match[0] == "COUNT":
                                        yield ["COUNT", function_match[-1]]
                                return "búsqueda completa"
                                # for compatibility
                        yield next(read)
                        for row in read:
                            if row and re.search(f"^.*{re.escape(search) if not escaped else search}.*$",
                                                 "".join(row[1:]), re.IGNORECASE) is not None:
                                yield row
                else:
                    for row in read:
                        if row:
                            yield row

    def guardar_datos_csv(self, enforce_unique=None) -> str:
        """método publico guardar_datos_csv
        permite escribir una nueva entrada en un archivo csv y retornar la nueva entrada añadida

        Argumentos:

        - enforce unique puede ser None o una tuple con str, permite decidir si
        el valor del o los atributos de la clase debe ser único con respecto a los presentes en el csv,
        si es tuple debe ser de la siguiente forma ('nombre_atributo',) para un atributo y
        ('nombre_atributo1', 'nombre_atributo2', 'nombre_atributo3') para multiples ideal si se guardan
        atributos de solo una clase o un conjunto de clases con un padre y atributos en común, si su
        valor es None no se chequea que el atributo deba ser único

        Valor de retorno:

        - un str de la nueva entrada creada y si se especifico enforce unique y se encontró
        que la entrada a guardar ya estaba presente se retorna el str presente

        Excepciones:

        - ValueError si el valor del argumento no es el apropiado o si en modo single = True
        se intenta guardar una entrada con más valores (atributos) que la cantidad de columnas
        disponibles o con nombres de valores que no sean iguales a los ya presentes (nombre columnas)
        """
        if not self.can_save:
            return (f"\nAdvertencia: Actualmente esta ocupando un objeto de tipo {type(self.object).__name__}"
                    "el cual no posee un __dict__ por lo que es imposible guardar entradas con él")
        # self.current_rows can't be less than zero so even if self.max_row_limit
        # is negative (truthy) the firs condition still checks
        if self.current_rows - 1 >= self.max_row_limit or not self.max_row_limit:
            return ("\nAdvertencia: Su entrada no fue creada ya que para mantener la eficiencia de este programa "
                    f"recomendamos\nlimitar el numero de entrada a {self.max_row_limit - 3_000} "
                    f"favor de ir a\n{self.file_path}\nhacer una, copia reiniciar el programa y\n"
                    "borrar todas las entradas para proseguir normalmente\nde aquí en adelante "
                    "solo se aceptaran operaciones de lectura y borrado de entradas solamente")

        if enforce_unique is not None and self.current_rows > 1:
            if not isinstance(enforce_unique, tuple):
                raise ValueError(
                    f"el parámetro enforce_unique debe ser una tuple pero fue {type(enforce_unique).__name__}")
            elif not enforce_unique:
                raise ValueError(f"la tuple debe contener al menos un str")
            elif not all([isinstance(item, str) for item in enforce_unique]):
                raise ValueError(
                    "la tuple solo debe contener str "
                    f"su tuple contiene {', '.join([str(type(item).__name__) for item in enforce_unique])}")
            # strip("_") para eliminar el _ que es puesto cuando
            # se tiene atributos que usan algún tipo decorador como
            # el property y setter
            # asi puedo buscar en atributos no consecutivos para ver si son únicos
            if not self.single and str(self.object.__class__) in self.current_classes:
                vals_to_check: str = ".+".join([re.escape(f'{str(key).strip("_")}: {re.sub(":", ";", str(val))}') for
                                                key, val in self.object.__dict__.items() if
                                                str(key).strip("_") in enforce_unique])
                for entry in self.leer_datos_csv(search=vals_to_check, back_up=True, escaped=True):
                    # esto por si el csv contiene elementos de distintas clases
                    if entry[1] == str(self.object.__class__):
                        return "presente"
            elif self.single:
                vals_to_check = " | ".join([f'"{str(key).strip("_")}" = {val}' for
                                            key, val in self.object.__dict__.items() if
                                            str(key).strip("_") in enforce_unique])
                skip_first = self.leer_datos_csv(search=vals_to_check, back_up=True)
                next(skip_first)
                for _ in skip_first:
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
                                self.new_head = [self.header[0],
                                                 *[str(key).strip("_").upper() for key in self.object.__dict__ if
                                                   str(key).strip("_") in self.exclude]]
                                write.writerow(self.new_head)
                            else:
                                self.new_head = [self.header[0],
                                                 *[str(key).strip("_").upper() for key in self.object.__dict__ if
                                                   str(key).strip("_") not in self.exclude]]
                                write.writerow(self.new_head)
                        else:
                            self.new_head = [self.header[0],
                                             *[str(key).strip("_").upper() for key in self.object.__dict__]]
                            write.writerow(self.new_head)
            self.current_rows += 1
        if self.exclude is not None:
            if self.exclude[0] == "!":
                if not self.single:
                    class_repr = ", ".join([f'{str(key).strip("_")}: {re.sub(":", ";", str(val))}' for key, val in
                                            self.object.__dict__.items() if str(key).strip("_") in self.exclude])
                else:
                    class_repr = [(str(key).strip('_').upper(),
                                   str(val)) for key, val in self.object.__dict__.items()
                                  if str(key).strip("_") in self.exclude]
                    if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
                        raise ValueError("en modo single = True solo se permiten objetos "
                                         "con el mismo número de atributos y nombres "
                                         f"que el actual {', '.join(self.new_head)}")
            else:
                if not self.single:
                    class_repr = ", ".join([f'{str(key).strip("_")}: {re.sub(":", ";", str(val))}' for key, val in
                                            self.object.__dict__.items() if str(key).strip("_") not in self.exclude])
                else:
                    class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.object.__dict__.items()
                                  if str(key).strip("_") not in self.exclude]
                    if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
                        raise ValueError("en modo single = True solo se permiten objetos "
                                         "con el mismo número de atributos y nombres "
                                         f"que el actual {', '.join(self.new_head)}")
        else:
            if not self.single:
                class_repr = ", ".join([f'{str(key).strip("_")}: {re.sub(":", ";", str(val))}' for key, val in
                                        self.object.__dict__.items()])
            else:
                class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.object.__dict__.items()]
                if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
                    raise ValueError("en modo single = True solo se permiten objetos "
                                     "con el mismo número de atributos y nombres "
                                     f"que el actual {', '.join(self.new_head)}")
        for count, files in enumerate(self.accepted_files):
            with open(files, "a", newline="", encoding="utf-8") as csv_writer:
                write = csv.writer(csv_writer, delimiter=self.delimiter)
                if not count:
                    self.current_rows += 1
                if not self.single:
                    write.writerow([f"[{self.current_rows - 1}]", self.object.__class__, class_repr])
                else:
                    write.writerow([f"[{self.current_rows - 1}]", *[val[1] for val in class_repr]])
        if not self.single:
            if str(self.object.__class__) not in self.current_classes:
                self.current_classes.append(str(self.object.__class__))
            return (f"\n{f'{self.delimiter}'.join(self.header)}\n[{self.current_rows - 1}]"
                    f"{self.delimiter}{self.object.__class__}{self.delimiter}{class_repr}")
        else:
            return (f"\n{f'{self.delimiter}'.join([*self.new_head])}\n[{self.current_rows - 1}]"
                    f"{self.delimiter}{f'{self.delimiter}'.join([val[1] for val in class_repr])}")

    # only a generator just in case there are way to many items to delete
    def borrar_datos(self, delete_index="", rewrite=False) -> Generator[str, None, str]:
        """método publico borrar_datos
        permite borrar las entradas seleccionadas del archivo csv

        Argumentos:

        - delete_index str que especifica que entradas a borrar en modo single = True
        los valores validos para borrar entradas son 'borrar todo' o alguno de los
        patrones validos establecidos por el método estático return_pattern, en modo
        single = False también se puede introducir el nombre literal de una clase para
        borrar todo los miembros de esa clase
        - rewrite bool que determina si al introducir 'borrar todo' se vuelva a copiar
        los contenidos del backup correspondiente al modo actual

        Valor de retorno:

        - un generador que devuelve de una en una las entradas borradas si alguna se borro
        como un str

        Excepciones:

        - ValueError si alguno de los argumentos no es del tipo esperado o si se introduce
        un formato para el argumento delete_index que no devuelva algún valor del csv
        """
        if not isinstance(delete_index, str):
            raise ValueError(f"el argumento delete_index debe ser str pero fue {type(delete_index).__name__}")
        if not isinstance(rewrite, bool):
            raise ValueError(f"el argumento rewrite debe ser bool pero fue {type(rewrite).__name__}")
        if delete_index == "borrar todo":
            if rewrite:
                with open(self.file_path, "w", newline="", encoding="utf-8") as _:
                    pass
                with open(self.file_path, "w", newline="", encoding="utf-8") as write_filter:
                    filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                    for entry in self.leer_datos_csv(back_up=True):
                        filter_.writerow(entry)
                yield "rewrite"
                # return only produces a
                # StopIterationError if you
                # use next after there are no more items
                # the value in the return is the message
                # passed to the error
                return "copiado desde el respaldo completado"
            if self.current_rows <= 1:
                yield "nada"
                return "no hay datos para borrar"
            for files in self.accepted_files:
                with open(files, "w", newline="", encoding="utf-8") as _:
                    pass
            self.current_rows = 0
            yield "todo"
            return "todos los items ya se borraron"
        else:
            if self.current_rows <= 1:
                yield "nada"
                return "no hay datos para borrar"
            if not self.single and delete_index in self.current_classes:
                yield f"{self.delimiter}".join(self.header if not self.single else [self.header[0], *self.new_head])
                with open(self.file_path, "w", newline="", encoding="utf-8") as class_deleter:
                    class_remover = csv.writer(class_deleter, delimiter=self.delimiter)
                    count: int = 1
                    row_getter: Generator[list[str], None, str] = self.leer_datos_csv(back_up=True)
                    class_remover.writerow(next(row_getter))
                    for item in row_getter:
                        if item[1] != delete_index:
                            item[0] = f"[{count}]"
                            class_remover.writerow(item)
                            count += 1
                        else:
                            yield f"{self.delimiter}".join(val for val in item)
            # use regex to accept multiple entries to delete
            elif isinstance(to_delete := self.return_pattern(delete_index), tuple):
                # to get rid of things like 00 or 03, 056
                operation: str | None = to_delete[0]
                vars_to_delete: list = [num for num in to_delete[-1] if self.current_rows >= num >= 0]
                if not vars_to_delete:
                    raise ValueError("ninguno de los valores ingresados corresponde al indice de alguna entrada")
                yield f"{self.delimiter}".join(self.header if not self.single else [self.header[0], *self.new_head])
                if operation == ":":
                    if len(vars_to_delete) == 1:
                        vars_to_delete.append(self.current_rows)
                    vars_to_delete.append("range")
                else:
                    vars_to_delete = [f"[{num}]" for num in vars_to_delete]

                # copying all the data except entries to delete from backup file to main file
                with open(self.file_path, "w", newline="", encoding="utf-8") as write_filter:
                    filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                    count: int = 1
                    mark: str = vars_to_delete[-1]
                    row_generator: Generator[list[str], None, str] = self.leer_datos_csv(back_up=True)
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
                                yield f"{self.delimiter}".join(val for val in entry)

                        else:
                            if entry[0] not in vars_to_delete:
                                entry[0] = f"[{count}]"
                                filter_.writerow(entry)
                                count += 1
                            else:
                                yield f"{self.delimiter}".join(val for val in entry)
            else:
                raise ValueError("utilize uno de los siguientes formatos para borrar una entrada:\n"
                                 "[n], [n:m], [n:], [n-m-p] (hasta 10) remplazando las letras por el indice "
                                 "de lo que desee eliminar"
                                 f"{'' if self.single else ' o introduciendo el nombre completo de una clase'}")
            # deleting all data on backup (is not up to date)
            # synchronizing backup
            # should I use asyncio?
            count = 0
            with open(str(self.backup_multi if not self.single else self.backup_single),
                      "w", newline="", encoding="utf-8") as write_filter:
                filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                for entry in self.leer_datos_csv():
                    filter_.writerow(entry)
                    count += 1
            if not self.single:
                self.current_classes.clear()
            self.current_rows = self.__len__()

    # static method
    @staticmethod
    def return_pattern(str_pattern) -> tuple | None:
        """ método estático publico return_pattern

        Argumento:

        - str_pattern el str en el cual se buscara el patron deseado
        este método específicamente sirve para saber si el usuario introduce el patron
        correcto para buscar o eliminar alguna entrada del csv, en nuestro caso el usuario
        debe ingresar ya sea [numero], [numero:], [numero1:numero2] o [numero1-numero2-numero3]

        Valor de retorno:

        - una tuple donde el primer valor es el tipo de operación, ya sea
        ':' (por rango), '-' (más de un valor especifico) o None (solo un valor especifico) y el
        segundo valor es una lista con los valores a eliminar. El valor de retorno es None si
        no se encontró el patron en el str pattern

        Excepciones:

        - ValueError si el argumento requerido no es del tipo solicitado
        """
        if not isinstance(str_pattern, str):
            raise ValueError(
                f"debe ingresar un str donde buscar un patron, pero se introdujo {type(str_pattern).__name__}")
        regex_obj = re.search(r"^\[(?:\d+(:)\d*|(?:\d+(-)){0,9}\d+)\]$", str_pattern)
        if regex_obj is not None:
            separator: list = [sep for sep in regex_obj.groups() if sep is not None]
            str_pattern = re.sub(r"[\[\]]", "", str_pattern)
            if separator:
                pattern_nums: list[int] = [int(val) for val in str_pattern.split(separator[0]) if val]
                pattern_nums.sort()
                return separator[0], pattern_nums
            return None, [int(str_pattern), ]
        return None

    def __query_parser(self, string_pattern) -> list:
        """ método privado __query_parser
        permite aplicar operaciones lógicas (>=, <=, <, >, =, !=) a las búsquedas
        del usuario dando más posibilidades al momento de filtrar datos

        Argumento:

        - string_pattern str introducida por el usuario la cual sera ocupada
        para hacer las comparaciones requeridas a la hora de buscar datos el formato
        general debe ser '"nombre_columna" operador_lógico valor'

        Valor de retorno:

        - una lista con los tokens a utilizar para filtrar resultados si se encontró un
        patron de otra forma un lista vacía

        Excepciones:

        - AttributeError si se intenta ocupar este método en el modo equivocado
        - ValueError si el tipo del argumento requerido no es el indicado
        """
        if not self.single:
            raise AttributeError("opción de filtrado por selector lógico no disponible en modo single = False")
        if not isinstance(string_pattern, str):
            raise ValueError(
                f"el tipo del argumento string_pattern debe ser str pero fue {type(string_pattern).__name__}")
        operation_hash_table = {">=": lambda x, y: x >= y, "<=": lambda x, y: x <= y,
                                "<": lambda x, y: x < y, ">": lambda x, y: x > y,
                                "=": lambda x, y: x == y, "!=": lambda x, y: x != y}
        query_regex: str = (r'^(?:!?\[([^\[,\s><=\|&!:"+*-\.\'/\?]+)*\] )?"([^\s,><=\|&!":+*-\.\'#/\?]+)" '
                            r'(>=|>|<=|<|=|!=) (.+?)')
        query_regex += r"(?: (\||&) "
        query_regex += r"(?: (\||&) ".join([r'"([^,\s><=\|&!:"+*-\.\'#/\?]+)" (>=|>|<=|<|=|!=) (.+?))?'
                                            for _ in range(0, 3)])
        query_regex += r'(?:~((?:AVG|MAX|MIN|SUM|COUNT|LIMIT|ASC|DESC|UNIQUE):(?:[^:,\s><=!\|&"+*\'#/\?]+)*))?$'
        new_pattern = re.search(query_regex, string_pattern)
        if new_pattern is not None:
            valid_tokens = list(filter(None, new_pattern.groups()))
            function_group = []
            if any([val in valid_tokens[-1] for val in
                    ("AVG:", "MAX:", "MIN:", "COUNT:", "SUM:", "LIMIT:", "ASC:", "DESC:", "UNIQUE:")]):
                function_group.append(valid_tokens.pop())
            exclude_group = []
            for exclude in valid_tokens:
                if exclude in ("<=", ">=", ">", "<", "=", "!="):
                    exclude_group.pop()
                    break
                else:
                    exclude_group.append(exclude)
            if exclude_group:
                valid_tokens.pop(0)
            contents: list = []
            sub_queries: list = []
            for count, token in enumerate(valid_tokens, 1):
                if count % 4 == 0:
                    new_query: list = [] + sub_queries
                    contents.append(new_query)
                    contents.append(token)
                    sub_queries.clear()
                else:
                    sub_queries.append(token)
            contents.append([] + sub_queries)
            sub_queries.clear()
            next_token: bool = True
            for query in contents:
                if isinstance(query, list) and query:
                    if str(query[0]).upper() in self.new_head:
                        try:
                            sub_queries.append([str(query[0]).upper(), operation_hash_table[query[1]], query[2]])
                        # query has syntax error that produce
                        # operations like a > 2 to end like a >
                        except IndexError:
                            return []
                        if not next_token:
                            next_token = True
                    else:
                        next_token = False
                        try:
                            if isinstance(sub_queries[-1], str):
                                sub_queries.pop()
                        except IndexError:
                            pass
                else:
                    if next_token:
                        sub_queries.append(query)
            if exclude_group:
                exclude_group[0] = tuple(
                    (self.new_head.index(str(item).upper()) for item in exclude_group[0].split("#") if
                     str(item).upper() in self.new_head))
                if exclude_group[0]:
                    if string_pattern[0] != "!":
                        exclude_group[0] = tuple(
                            (val for val in range(1, len(self.new_head)) if val not in exclude_group[0]))
                    sub_queries.insert(0, exclude_group[0])
            if function_group:
                sub_queries.append(function_group[0])
            return sub_queries
        return []

    def export(self, destination, class_name) -> bool:
        """ método publico export
        permite crear csv individuales a partir de datos almacenados
        en el modo multiple

        Argumentos:

        - destination: una ruta valida a la cual serán escritos los datos exportados
        - class_name: los elementos seleccionados para exportar (debe ser el nombre literal
        que contiene los elementos bajo la columna CLASE)

        Valor de retorno:

        - True si se escribieron todos los datos correspondientes False si nada se escribió

        Excepciones

        - AttributeError si se accede a este método en el modo equivocado
        - ValueError si el valor de los argumentos no es valido
        - DataExportError si no calza la cantidad de valores del objeto a exportar
        con la cantidad de columnas disponibles o no se pudo exportar correctamente
        algún valor o crear correctamente los nombres de columnas
        """
        if self.single:
            raise AttributeError("operación no disponible en modo single = True")
        if not self.current_rows:
            return False
        if isinstance(destination, str):
            valid_path = Path(destination)
            if not valid_path.is_file():
                raise ValueError(f"el archivo de destino debe ser una ruta valida pero fue {valid_path}")
            elif valid_path.suffix != ".csv":
                raise ValueError(f"el archivo de destino debe ser de extension .csv pero fue {valid_path.suffix}")
        else:
            raise ValueError(
                f"el tipo para el argumento destination debe ser str pero fue {type(destination).__name__}")
        if not isinstance(class_name, str):
            raise ValueError(f"el tipo para el argumento class_name debe ser str pero fue {type(class_name).__name__}")
        if class_name not in self.current_classes:
            return False
        lines_to_export: Generator[list[str], None, str] = self.leer_datos_csv(search=class_name, back_up=True)
        # current headers
        next(lines_to_export)
        # take header from attribute
        headers: list = next(lines_to_export)
        head_names: list = re.findall(r"([^:,\s]+:)", headers[-1])
        if head_names:
            head_names = ["INDICE", ] + [str(item).strip(":").upper() for item in head_names]
        else:
            # for whatever reason head names couldn't be resolved
            raise DataExportError(
                f"no fue posible enviar los datos debido a un error de formato en el encabezado  en: {headers}")
        values: list = re.findall(r"([^:]+(?:,|$))", headers[-1])
        if values:
            values = ["[1]", ] + [str(val).strip(" ,") for val in values]
        else:
            raise DataExportError(
                f"no fue posible enviar los datos debido a un error de formato en los atributos en: {values}")
        if len(values) != len(head_names):
            raise DataExportError(
                f"la cantidad de atributos no corresponde a la cantidad de encabezados en {values} para {head_names}")
        with open(destination, "w", newline="", encoding="utf-8") as exporter:
            data_export = csv.writer(exporter, delimiter=self.delimiter)
            data_export.writerow(head_names)
            data_export.writerow(values)
            count = 2
            for remain_lines in lines_to_export:
                next_values: list = re.findall(r"([^:]+(?:,|$))", remain_lines[-1])
                if next_values:
                    next_values = [f"[{count}]", ] + [str(attr).strip(" ,") for attr in next_values]
                    count += 1
                    if len(next_values) != len(head_names):
                        raise DataExportError(
                            "la cantidad de atributos no corresponde a la cantidad de "
                            f"encabezados en {next_values} para {head_names}")
                    else:
                        data_export.writerow(next_values)
                else:
                    raise DataExportError(
                        "no fue posible enviar los datos debido a un error "
                        f"de formato en los atributos en: {next_values}")
            return True

    @classmethod
    def index(cls, file_path, delimiter, id_present=True) -> None:
        """ método de clase index
        permite agregar indices con el formato de este programa y
        limpiar los encabezados par que puedan ser utilizados, siempre
        asume que el archivo tendrán un encabezado de otra forma los
        resultados serán inesperados, una ves reescrito el archivo este
        sera copiado al backup_single del programa, idealmente solo para
        leer archivos csv que no sean generados a base de clases de python,
        para escribir nuevos datos a este tipo de archivos se debe crear una
        clase la cual sirva de intermediario para ingresarlos, los
        caracteres reservados para encabezados son: ,><=|&!":+*-.'#/?, espacios en blanco,
        etc. (cualquier carácter que no pueda ser usado como nombre de variable o atributo)

        Argumentos:

        - file_path: ruta valida del archivo a leer para dar formato valido
        - delimiter: el delimitador del archivo csv actual
        - id_present: bool el cual indica que si solo se debe reescribir los id o
        si hay que crearlos desde cero

        Valor de retorno:

        - None

        Excepciones:

        - ValueError si algún tipo de los argumentos ingresados no fue el correcto
        """
        if not isinstance(id_present, bool):
            raise ValueError(
                f"el tipo esperado para el argumento id_present es bool pero fue {type(id_present).__name__}")
        new_class = cls(file_path, None, True, delimiter, check_hash=False)
        with open(file_path, "r", newline="", encoding="utf-8") as import_:
            new_import = csv.reader(import_, delimiter=new_class.delimiter)
            if id_present:
                head_file = [re.sub(r'[,><=\|&!\s:"]', "", item).upper() for item in next(new_import)]
            else:
                head_file = ["INDICE", ] + [re.sub(r'[,><=\|&!\s:+*-\.\'#/\?"]', "", item).upper() for item in
                                            next(new_import)]
            with open(new_class.backup_single, "w", newline="", encoding="utf-8") as write_backup:
                new_back_up = csv.writer(write_backup, delimiter=new_class.delimiter)
                new_back_up.writerow(head_file)
                for count, line in enumerate(new_import, 1):
                    if count > new_class.max_row_limit:
                        break
                    if id_present:
                        line[0] = f"[{count}]"
                        new_back_up.writerow(line)
                    else:
                        new_back_up.writerow([f"[{count}]", ] + line)

    def __len__(self) -> int:
        with open(str(self.backup_multi if not self.single else self.backup_single),
                  "r", newline="", encoding="utf-8") as csv_reader:
            read = csv.reader(csv_reader, delimiter=self.delimiter)
            # clever method to get the len and avoid putting
            # all the contents of the file in memory
            # this 1 for _ in read is an iterator
            count = 0
            if not self.single and not self.current_classes:
                for item in read:
                    count += 1
                    if item[1] not in self.current_classes:
                        self.current_classes.append(item[1])
                if count:
                    self.current_classes = self.current_classes[1:]
                return count
            return sum(1 for _ in read)


class DataExportError(Exception):
    """ clase DataExportError es una excepción creada
    para representar errores a la hora de exportar archivos
    en modo single = False relacionados en este contexto a errores
    de formato o cantidad de atributos inapropiados para un mismo
    conjunto de clases a exportar
    """

    def __init__(self, message) -> None:
        self.message_error = message

    @property
    def message_error(self) -> str:
        return self._message_error

    @message_error.setter
    def message_error(self, value) -> None:
        if isinstance(value, str):
            self._message_error = value
        else:
            raise ValueError(f"el contenido del error debe ser str pero fue {type(value).__name__}")

    def __str__(self) -> str:
        return self.message_error
