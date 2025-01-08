import csv
import re
import hashlib
from typing import Generator, Callable, Any
from math import isnan, ceil, floor
from pathlib import Path
from datetime import date, timedelta
from keyword import iskeyword
from dataclasses import make_dataclass
from random import randint

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
        self.hashing = check_hash

        self.can_save = True
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
        # if self.hashing is set to false the len of the current
        # contents of the backup is use
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
            # check that all names of headers are different
            # this is how you do type pattern matching
            case tuple((str(), str(), str())):
                if len(set([val.lower() for val in value])) == 3:
                    self._header = value
                else:
                    raise ValueError(f"el valor debe ser una tuple con 3 str distintos pero fue {value}")
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
    def leer_datos_csv(self, search="", back_up=False, escaped=False, query_functions=True) -> Generator[list[str] | str, None, str]:
        """método publico leer_datos_csv

        Argumentos:

        - search es la str que se usa para buscar dentro del csv
        - back_up es para buscar en el csv de backup envés del original
        - escaped es para saber si se debe escapar algún carácter especial
        en el search str cuando se realize la búsqueda mediante expresiones regulares
        - query_function es para determinar si se debe aplicar o no las funciones
        pasadas en una query

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
                                    if query_functions:
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
                                    else:
                                        list_of_match.pop()
                                for row in read:
                                    bool_values: list = []
                                    # add function to get the last or first letter and write test
                                    # [= is the str star with operator and ]= is the str end with operator
                                    # for a = "hola" a[5] gives index error but a[5:] or a[5:10] gives ""
                                    operation_hash_table: dict[str, Callable] = {
                                    ">=": lambda x, y: x >= y, "<=": lambda x, y: x <= y,
                                    "<": lambda x, y: x < y, ">": lambda x, y: x > y,
                                    "=": lambda x, y: x == y, "!=": lambda x, y: x != y,
                                    "[=": lambda x, y: str(y).lower() in str(x)[0:len(str(y))].lower(), 
                                    "]=": lambda x, y: str(y).lower() in str(x)[-len(str(y)):].lower(),
                                    }
                                    for element in list_of_match:
                                        if isinstance(element, list):
                                            try:
                                                head_index = self.new_head.index(element[0])
                                            except IndexError:
                                                # error if col that logical operator is applied to 
                                                # is not a valid col name (does not exist)
                                                yield "error de sintaxis"
                                                return "sintaxis no valida búsqueda terminada"
                                            # so you can search from index to
                                            if head_index > 0:
                                                val = row[head_index]
                                            else:
                                                # for suing [= ]= with index you don't have to consider []
                                                val = re.sub(r"[\[\]]", "", row[head_index])
                                            # should always be present on dict no need for get
                                            parse_function: Callable = operation_hash_table[element[1]] 
                                            if element[1] in ("]=", "[=",):
                                                # this operator only accept values as str no matter if both can
                                                # be numbers of dates otherwise when parsing to float the comparison
                                                # may be False when is True
                                                try:
                                                    bool_values.append(parse_function(val, element[-1]))
                                                except TypeError:
                                                    bool_values.append(False)
                                            else:
                                                try:
                                                    row_val, expected_val = float(val), float(element[-1])
                                                except ValueError:
                                                    pass
                                                else:
                                                    bool_values.append(parse_function(row_val, expected_val))
                                                    continue
                                                try:
                                                    # the only standard I will support
                                                    row_time = date.fromisoformat(val) 
                                                    expected_time = date.fromisoformat(element[-1])
                                                except ValueError:
                                                    pass
                                                else:
                                                    bool_values.append(parse_function(row_time, expected_time))
                                                    continue
                                                try:
                                                    bool_values.append(parse_function(val, element[-1]))
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
                                                else:
                                                    break
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
        los valores validos para borrar entradas son 'borrar todo', alguno de los
        patrones validos establecidos por el método estático return_pattern o una query
        valida para buscar datos que sea de estructura DELETE ON <query búsqueda>. En modo
        single = False también se puede introducir el nombre literal de una clase para
        borrar todo los miembros de esa clase además de 'borrar todo' y patrones definidos
        en return_pattern
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
                    row_getter: Generator[list[str] | str, None, str] = self.leer_datos_csv(back_up=True)
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
                    row_generator: Generator[list[str] | str, None, str] = self.leer_datos_csv(back_up=True)
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
            elif (regex_delete := re.search(r'^DELETE ON (.+?)$', delete_index)) is not None and self.single:
                delete_on = self.leer_datos_csv(search=regex_delete.group(1), back_up=True, query_functions=False)
                # header is not required
                next(delete_on)
                to_delete: list = []
                for entry_index in delete_on:
                    if isinstance(entry_index, str):
                        yield entry_index
                        return "sintaxis no valida operación cancelada"
                    to_delete.append(entry_index[0])
                if not to_delete:
                    yield "no se encontraron entradas para eliminar"
                    return "sintaxis valida pero sin entradas seleccionadas para la operación"

                with open(self.file_path, "w", newline="", encoding="utf-8") as delete_query:
                    deleter = csv.writer(delete_query, delimiter=self.delimiter)
                    reader: Generator[list[str] | str, None, str] = self.leer_datos_csv(back_up=True)
                    yield f"{self.delimiter}".join([self.header[0], *self.new_head])
                    deleter.writerow(next(reader))
                    counter: int  = 1
                    for entry in reader:
                        if entry[0] not in to_delete:
                            entry[0] = f"[{counter}]"
                            deleter.writerow(entry)
                            counter += 1
                        else:
                            yield f"{self.delimiter}".join(val for val in entry)
            else:
                message = """"utilize uno de los siguientes formatos para borrar una entrada:\n
                              [n], [n:m], [n:], [n-m-p] (hasta 10) remplazando las letras por el indice\n
                              "de lo que desee eliminar """
                if not self.single:
                    raise ValueError(message + "o introduciendo el nombre completo de una clase")
                else:
                    raise ValueError(message + " o escribiendo una consulta usando la palabra clave DELETE para selecciones más complejas")
            # deleting all data on backup (is not up to date)
            # synchronizing backup
            # should I use asyncio?
            with open(str(self.backup_multi if not self.single else self.backup_single),
                      "w", newline="", encoding="utf-8") as write_filter:
                filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                for entry in self.leer_datos_csv():
                    filter_.writerow(entry)
            if not self.single:
                self.current_classes.clear()
            self.current_rows = self.__len__()

    def actualizar_datos(self, update_query) -> Generator[dict | str, None, str]:
        """método publico actualizar_datos

        Argumentos:

        - update_query es la str utilizada para determinar que columnas y a que valor
        actualizarlas y en que filas

        Valor de retorno:

        - un generador que retorna ya sea errores de sintaxis, una advertencia o de una en
        una las filas que fueron actualizadas

        Excepciones:

        - AttributeError si se intenta ocupar en modo single=False
        - ValueError si los tipos de los argumentos no son los apropiados
        """
        if not self.single:
            raise AttributeError("no es posible actualizar datos en el modo actual single = True ya que no posee dicha opción")
        if not self.current_rows:
            raise ValueError("no es posible actualizar si no hay valore disponibles")
        if not self.can_save:
            yield (f"\nAdvertencia: Actualmente esta ocupando un objeto de tipo {type(self.object).__name__}"
                   "el cual no posee un __dict__ por lo que es imposible actualizar sus entradas")
            return "acción denegada"
        if not isinstance(update_query, str):
            raise ValueError(
                f"debe ingresar un str como instrucción para actualizar valores, pero se introdujo {type(update_query).__name__}")
        # this works but use .strip() on the values to update
        pattern = (r'^UPDATE:~"([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?)(?: "([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?))?'
                   r'(?: "([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?))?(?: "([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?))? ON (.+?)$')
        regex_update = re.search(pattern, update_query)
        if regex_update is not None:
            value_tokens = list(filter(None, regex_update.groups()))
            where_update = value_tokens.pop()
            search_result: Generator[list[str] | str, None, str] = self.leer_datos_csv(search=where_update, back_up=True, query_functions=False)
            head_update: list[str] = next(search_result)
            col_index: list = []
            row_index: list = []
            update_functions_with_args: str = r'^%(?:(REPLACE|RANDOM-INT):~(.+?)#(.+))|%(?:(ADD|SUB|MUL|DIV|NUM-FORMAT):~(.+))$'
            for update_col in value_tokens:
                col, col_val  = update_col.split(sep="=", maxsplit=1)
                # this is mostly to keep the query syntax more consistent
                # remember that headers are on uppercase
                col: str = col.replace('"', '').upper()
                if col not in head_update:
                    yield "error de sintaxis la columna a actualizar debe estar dentro de la consulta de búsqueda"
                    return "sintaxis no valida operación cancelada"
                # can't be head_update because if is shorter than the unfiltered header of the csv
                # it is going to end up updating the wrong value
                val_index: int = self.new_head.index(col)
                if not val_index:
                    yield "error de sintaxis no se puede actualizar el valor del indice"
                    return "sintaxis no valida operación cancelada"
                if (val_function := re.search(update_functions_with_args, col_val)) is not None:
                    branch_groups = list(filter(None, val_function.groups()))
                    col_index.append((val_index, [f"%{branch_groups[0]}", *branch_groups[1:]]))
                else:
                    col_index.append((val_index, col_val))
            # it is safe to pass an exhausted generator to a for loop
            # the exception is managed automatically
            for item in search_result:
                if isinstance(item, str):
                    yield item
                    return "sintaxis no valida operación cancelada"
                row_index.append(item[0])
            if not row_index:
                yield "no se encontraron entradas para actualizar"
                return "sintaxis valida pero sin entradas seleccionadas para la operación"
            # READ FROM BACKUP AND WRITE TO ORIGINAL
            # THEN DELETE BACKUP AND WRITE ORIGINAL TO BACKUP
            update_functions: dict[str, Callable] = {
                "%UPPER": lambda x: str(x).upper(), "%LOWER": lambda x: str(x).lower(), 
                "%TITLE": lambda x: str(x).title(), "%CAPITALIZE": lambda x: str(x).capitalize(),
                "%REPLACE": lambda x, old, new: str(x).replace(old, new),
                "%ADD": lambda x, y: str(x + y), "%SUB": lambda x, y: str(x - y),
                "%MUL": lambda x, y: str(x * y), "%DIV": lambda x, y: str(x) if not y else str(x/y),
                "%RANDOM-INT": lambda x, y: str(randint(x, y)) if x < y else str(randint(y, x)),
                "%CEIL": lambda x: str(ceil(x)), "%FLOOR": lambda x: str(floor(x)),
                "%NUM-FORMAT": lambda x, y: f"{x:.{y}f}",}
            
            # for the case that any value was not updated due to impossible update function
            # use due to incorrect expected types for arguments
            was_updated: int = 0
            with open(self.file_path, "w", newline="", encoding="utf-8") as write_update:
                updater = csv.writer(write_update, delimiter=self.delimiter)
                reader: Generator[list[str] | str, None, str] = self.leer_datos_csv(back_up=True)
                updater.writerow(next(reader))
                for count, entry in enumerate(reader, start=1):
                    if f"[{count}]" in row_index:
                        # make this the return value, if str is yield create a new key with
                        # the col name and set its value to list, you should use a list in case
                        # one col has multiple errors, by convention col names always in uppercase
                        # IF error is present you delete the last old if no old is found then that row do not suffered
                        # any changes
                        operations_status: dict[str, list | dict[str, list]] = {"result": [], "errors": {}, "old": {}}
                        for position, new_val in col_index:
                            # start appending the old value and remove it if an error is appended
                            if operations_status["old"].get(self.new_head[position], None) is None:
                                operations_status["old"][self.new_head[position]] = [entry[position],]
                            else:
                                operations_status["old"][self.new_head[position]].append(entry[position])
                            if operations_status["errors"].get(self.new_head[position], None) is None:
                                operations_status["errors"][self.new_head[position]] = []
                            if new_val in ("%UPPER", "%LOWER", "%TITLE", "%CAPITALIZE"):
                                entry[position] = update_functions[new_val](entry[position])
                                was_updated += 1
                            # NAN AND INF DO NOT PASS THIS TRY THEY RAISE VALUE ERROR
                            elif new_val in ("%CEIL", "%FLOOR"):
                                try:
                                    entry[position] = update_functions[new_val](float(entry[position]))
                                except ValueError:
                                    operations_status["old"][self.new_head[position]].pop()
                                    operations_status["errors"][self.new_head[position]].append(f"solo es posible aplicar la función {new_val} a números y su valor fue {entry[position]}")
                                else:
                                    was_updated += 1
                            elif isinstance(new_val, list):
                                if new_val[0] in ("%REPLACE",):
                                    entry[position] = update_functions[new_val[0]](entry[position], *new_val[1:])
                                    was_updated += 1
                                elif new_val[0] == "%RANDOM-INT":
                                    try:
                                        limit_1 = int(new_val[1])
                                        limit_2 = int(new_val[-1])
                                    except ValueError:
                                        operations_status["old"][self.new_head[position]].pop()
                                        operations_status["errors"][self.new_head[position]].append(("para usar la función %RANDOM-INT debe pasar dos números enteros como limites inferior "
                                                                                                    f"y superior pero introdujo {new_val[1]} y {new_val[-1]} entrada [{count}] no actualizada"))
                                    else:
                                        entry[position] = update_functions["%RANDOM-INT"](limit_1, limit_2)
                                        was_updated += 1
                                elif new_val[0] == "%NUM-FORMAT":
                                    parse_values: list = []
                                    for count, callable_item, argument_item in ((0, float, entry[position]), (1, int, new_val[-1])):
                                        descriptor: str = "ocupar una columna que contenga valores decimales o enteros" if not count else "pasar como argumento un número entero"
                                        try:
                                            parsed_val: float | int = callable_item(argument_item)
                                        except ValueError:
                                            operations_status["old"][self.new_head[position]].pop()
                                            operations_status["errors"][self.new_head[position]].append((f"para usar la función %NUM-FORMAT debe {descriptor} "
                                                                                                         f"pero el valor fue de tipo {type(argument_item).__name__}"))
                                        else: 
                                            parse_values.append(parsed_val)
                                    if len(parse_values) == 2:
                                        # consider for future release a config file to 
                                        # allow easier manipulation of global variables like 25
                                        # (formatting limit for float)
                                        # PASSING 0 TO %INT-FORMAT IS LIKE USING %CEIL range is 1 to 25
                                        # since we already have a floor function
                                        if not (25 >= parse_values[-1] > 0):
                                            operations_status["old"][self.new_head[position]].pop()
                                            operations_status["errors"][self.new_head[position]].append(("el segundo argumento para la función %NUM-FORMAT "
                                                                                                        f"debe ser un número entre 1 y 25 pero fue {parse_values[-1]}"))
                                        else:
                                            entry[position] = update_functions[new_val[0]](*parse_values)
                                            was_updated += 1
                                else:
                                    # CHECK IF IS FLOAT OR STR TO DO THE REQUIRED OPERATIONS
                                    # TEST ON REPLACE IN FLOAT AND THEN IN THAT FLOAT TEST ONE OF THE OTHER FUNCTIONS
                                    # CHECK IF NO ENTRIES WHERE UPDATED DUE TO NOT MEETING THE CONDITIONS FOR THE
                                    # FUNCTIONS
                                    # you can do operations between dates but is better to do operations
                                    # between dates and umbers since you can't sum dates but you can sum and
                                    # subtract numbers from dates
                                    # test for float when is nan and float if div is passed a 0
                                    # test if it fails for a column but it does not for another

                                    # this code is for allowing to do arithmetics with another value of the same row
                                    value_for_col: str = new_val[-1]
                                    if (use_another := re.search(r"^USE:~(.+)$", new_val[-1])) is not None:
                                        # to not consider the index
                                        if (other_col_val := use_another.group(1).upper()) in self.new_head[1:]:
                                            value_for_col: str = entry[self.new_head.index(other_col_val)]
                                        else:
                                            operations_status["old"][self.new_head[position]].pop()
                                            operations_status["errors"][self.new_head[position]].append(("el selector USE:~ solo se puede usar para pasar como argumento el valor de otra columna en la fila a la función "
                                                                                                f"seleccionada por lo que el valor debe ser el nombre de una columna ({self.new_head[1:]}) pero fue {other_col_val}"))
                                            continue
                                    for possibly_type in ([float, float], [date.fromisoformat, int], [str, str]):
                                        try:
                                            cast_current_value: float | date | str = possibly_type[0](entry[position])
                                            cast_function_value: float | int | str = possibly_type[1](value_for_col)
                                        except ValueError:
                                            pass
                                        else:
                                            if isinstance(cast_current_value, float):
                                                # nan and inf operations are defined internally for float
                                                entry[position] = update_functions[new_val[0]](cast_current_value, cast_function_value)
                                                was_updated += 1
                                            elif isinstance(cast_current_value, date):
                                                if new_val[0] in ("%ADD", "%SUB"):
                                                    if 0 < cast_function_value <= 1_000:
                                                        entry[position] = update_functions[new_val[0]](cast_current_value, timedelta(days=cast_function_value))
                                                        was_updated += 1
                                                    else:
                                                        operations_status["old"][self.new_head[position]].pop()
                                                        operations_status["errors"][self.new_head[position]].append(("superado el número de días que se puede añadir o restar a una fecha "
                                                                                                                     f"(entre 1 y 1000) ya que su valor fue {cast_function_value} entrada [{count}] no actualizada"))
                                                else:
                                                    operations_status["old"][self.new_head[position]].pop()
                                                    operations_status["errors"][self.new_head[position]].append(("solo es posible aplicar una función %ADD o %SUB sobre una fecha "
                                                                                                                 f"y su elección fue {new_val[0]} entrada [{count}] no actualizada"))
                                            elif isinstance(cast_current_value, str):
                                                if new_val[0] == "%ADD":
                                                    entry[position] = update_functions[new_val[0]](cast_current_value, cast_function_value)
                                                    was_updated += 1
                                                else:
                                                    operations_status["old"][self.new_head[position]].pop()
                                                    operations_status["errors"][self.new_head[position]].append(("no se puede aplicar una función que no sea %ADD sobre un str "
                                                                                                                f"y su elección fue {new_val[0]} entrada [{count}] no actualizada"))
                                            break
                            # using regex is better for case insensitive col name reference handling
                            elif (copy_value := re.search(r"^%COPY:~(.+)$", new_val)) is not None:
                                if (target_copy := copy_value.group(1).upper()) in self.new_head[1:]:
                                    entry[position] = entry[self.new_head.index(target_copy)]
                                    was_updated += 1
                                else:
                                    operations_status["old"][self.new_head[position]].pop()
                                    operations_status["errors"][self.new_head[position]].append(("la función %COPY solo se puede usar para copiar el valor de una columna a otra para lo cual "
                                                                                                f"debe seleccionar el nombre de una columna, su valor fue {target_copy} pero las opciones son {self.new_head[1:]} "
                                                                                                f"entrada [{count}] no actualizada"))
                            else:
                                entry[position] = new_val
                                was_updated += 1
                        # fix this because if there is a str message then the entry is going to be
                        # pass to the user even if no changed was made (all cols yielded a str) or
                        # if only some cols where updated
                        # if all entries update have errors then we do not have old values since 
                        # they are still the same and in result we let know that no update was done

                        if sum(len(old_column) for old_column in operations_status["old"].values()):
                            operations_status["result"] = entry
                        else:
                            operations_status["result"] = [entry[0], "ningún valor de la fila fue actualizado, todas la operaciones fueron invalidas"]
                        yield operations_status
                    updater.writerow(entry)
            if was_updated:
                with open(str(self.backup_single), "w", newline="", encoding="utf-8") as rewrite_update:
                    rw_updater = csv.writer(rewrite_update, delimiter=self.delimiter)
                    for entry in self.leer_datos_csv():
                        rw_updater.writerow(entry)
        else:
            yield "error de sintaxis"

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
        query_regex: str = (r'^(?:!?\[([^\[,\s><=\|&!:"+*-\.\'/\?\]]+)*\] )?"([^\s,><=\|&!":+*-\.\'#/\?\[\]]+)" '
                            r'(>=|>|<=|<|=|!=|\[=|\]=) (.+?)')
        query_regex += r"(?: (\||&) "
        query_regex += r"(?: (\||&) ".join([r'"([^,\s><=\|&!:"+*-\.\'#/\?\[\]]+)" (>=|>|<=|<|=|!=|\[=|\]=) (.+?))?'
                                            for _ in range(0, 3)])
        query_regex += r'(?:~((?:AVG|MAX|MIN|SUM|COUNT|LIMIT|ASC|DESC|UNIQUE):(?:[^:,\s><=!\|&"+*\'#/\?\[\]]+)*))?$'
        new_pattern = re.search(query_regex, string_pattern)
        if new_pattern is not None:
            valid_tokens = list(filter(None, new_pattern.groups()))
            function_group = []
            if any([val in valid_tokens[-1] for val in
                    ("AVG:", "MAX:", "MIN:", "COUNT:", "SUM:", "LIMIT:", "ASC:", "DESC:", "UNIQUE:")]):
                function_group.append(valid_tokens.pop())
            exclude_group = []
            for exclude in valid_tokens:
                if exclude in ("<=", ">=", ">", "<", "=", "!=", "[=", "]="):
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
                            sub_queries.append([str(query[0]).upper(), query[1], query[2]])
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
        # at lest there must be a header and 1 entry
        # to prevent StopIteration when using next
        # just in case since the code below (class_name not in self.current_classes)
        # could catch the error went only you have the header (self.current_rows == 1) in a file 
        if self.current_rows < 2:
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

    # TODO IMPLEMENT LOGIC THAT ALLOWS TO ADD NEW COLUMNS UPON CREATION
    @classmethod
    def index(cls, file_path, delimiter, id_present=True, extra_columns = None, exclude = None) -> type:
        """ método de clase index
        permite agregar indices con el formato de este programa y
        crear un objeto para que pueda usarse de intermediario para
        agregar o actualizar las entradas del archivo, siempre
        asume que el archivo tendrán un encabezado de otra forma los
        resultados serán inesperados, una ves reescrito el archivo este
        sera copiado al backup_single del programa, para crear
        el objeto que permite añadir y actualizar campos se obtienen los nombres
        de atributos desde el encabezado del archivo pero si este no es valido
        (tiene cualquier carácter que no pueda ser usado como nombre de variable o atributo 
        en python) entonces no se podrá crear el objeto y un error es generado

        Argumentos:

        - file_path: ruta valida del archivo a leer para dar formato valido
        - delimiter: el delimitador del archivo csv actual
        - id_present: bool el cual indica que si solo se debe reescribir los id o
        si hay que crearlos desde cero
        - extra_columns: dict con las columnas a añadir al archivo y su valor por defecto
        - exclude: list con las columnas a excluir del archivo

        Valor de retorno:

        - un objeto creado de tipo CsvObjectWriter el cual puede ser usado para
        escribir o actualizar a un csv que no este asociado a una clase de python

        Excepciones:

        - ValueError si algún tipo de los argumentos ingresados no fue el correcto o
        si el encabezado del archivo tiene nombres de columna que no son nombres de
        atributos validos en python para crear el objeto
        """
        if not isinstance(id_present, bool):
            raise ValueError(
                f"el tipo esperado para el argumento id_present es bool pero fue {type(id_present).__name__}")
        new_cols = []
        default_vals = []
        excluded = []
        if extra_columns is not None:
            if isinstance(extra_columns, dict):
                new_cols = [str(key).upper() for key in extra_columns.keys()]
                default_vals = [str(value) if str(value) not in (" ", "") else "VOID" for value in extra_columns.values()]
            else:
                raise ValueError("si quiere añadir nuevas columnas debe pasar un argumento de tipo dict en extra_columns donde las llaves"
                                 "sean el nombre de la columna y el valor el valor por defecto que tendrá cada "
                                 f"fila para esa columna pero su argumento fue de tipo {type(extra_columns).__name__}")
        if exclude is not None:
            if isinstance(exclude, list):
                excluded.extend(set(str(val).upper() for val in exclude))
            else:
                raise ValueError("para excluir columnas existentes debe pasar un objeto de tipo list al argumento exclude que "
                                 f"contenga el nombre de las columnas a pasar pero su argumento fue de tipo {type(exclude).__name__}")
        # FOR EMPTY CSV FILE
        # you can't get the len by this method since is not going to give back the len
        # of the current file but the one already present in the backup
        # we use new_class mostly for file_path validation 
        new_class = cls(file_path, None, True, delimiter, check_hash=False)
        with open(file_path, "r", newline="", encoding="utf-8") as import_:
            new_import = csv.reader(import_, delimiter=new_class.delimiter)
            try:
                file_header = next(new_import)
            except StopIteration:
                raise ValueError("No es posible realizar la operación en un archivo sin contenidos")
            # IN BOTH CASES THE FIRST ITEM OF THE HEAD IS PASS APART FROM THE REST TO PREVENT
            # THE USER FROM EXCLUDING THE INDEX MANUALLY
            # IF YOU EXCLUDE EVERYTHING THE INDEX IS ALL YOU'RE GOING TO GET BACK
            if id_present:
                # like this only we exclude from the existing header so we avoid the case where the same name col
                # name is passed to the extra_columns and exclude arguments the intended behavior is to exclude
                # what already in the header not the extra columns that you pass after
                new_head: list[str] = [item.upper() for item in file_header[1:] if item.upper() not in excluded]
                head_file: list[str] = ["INDICE",] + file_header[1:]
            else:
                new_head: list[str] = [item.upper() for item in file_header if item.upper() not in excluded]
                head_file: list[str] = ["INDICE",] + file_header
             # TODO TEST WITH INDICE BUT EXCLUDING INDICE AND ID_PRESENT FALSE SHOULD RAISE NO ERROR
            excluded_values = [head_file.index(col) for col in head_file if col.upper() in excluded]
            head_file = [head_file[0],] + new_head
            # you need to calculate the index before adding new cols to the head
            # if id_present == False the INDICE becomes a reserved col name and valueError is raised
            head_file.extend(new_cols)
            if len(head_file) != len(set(head_file)):
                raise ValueError(f"los encabezados no pueden tener nombres repetidos para las columnas ({head_file})")
            # to not exclude index
            if 0 in excluded_values:
                excluded_values = [val for val in excluded_values if val != 0]
            if any([not val.isidentifier() or iskeyword(val) for val in head_file]):
                raise ValueError("el encabezado del archivo contiene caracteres inválidos para crear variables validas en python")
            with open(new_class.backup_single, "w", newline="", encoding="utf-8") as write_backup:
                new_back_up = csv.writer(write_backup, delimiter=new_class.delimiter)
                new_back_up.writerow(head_file)
                for count, line in enumerate(new_import, 1):
                    if count > new_class.max_row_limit:
                        break
                    if id_present:
                        line[0] = f"[{count}]"
                        line = [value for value in line if line.index(value) not in excluded_values]
                        new_back_up.writerow(line + default_vals)
                    else:
                        line = [f"[{count}]",] + line
                        new_back_up.writerow([value for value in line if line.index(value) not in excluded_values] + default_vals)
            return make_dataclass(cls_name="CsvObjectWriter", fields=[name.lower() for name in head_file[1:]])

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
