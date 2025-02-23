import csv
import re
import tempfile
from typing import Generator, Callable, Type, TextIO, Self
from math import isnan, ceil, floor
from pathlib import Path
from datetime import date, timedelta
from keyword import iskeyword
from dataclasses import make_dataclass
from random import randint
from inspect import isclass

# BY THIS APPROACH YOU MAKE IT SO YOU CAN ONLY ACCESS THE
# CLASS ATTRIBUTE FROM THE CLASS ITSELF NOT FROM AN INSTANCE
# IF SO  YOU GET A ATTRIBUTE ERROR
# TO KEEP THE BEHAVIOR CONSISTENT YOU NEED TO INHERIT FROM TYPE
# SINCE TYPE IS THE DEFAULT CLASS CONSTRUCTOR
class Meta(type):
    """ metaclass Meta para la clase BaseCsvManager su principal función
    es servir para definir getters and setters para los atributos de clase de BaseCsvManager"""

    # cantidad máxima de filas que puede tener un archivo csv
    _max_row_limit: int = 20_000
    # cantidad máxima de columnas que puede tener un archivo csv
    _max_col_limit: int = 15
    # cantidad máxima de caracteres que puede tener el nombre de un archivo csv
    _max_name_limit: int = 25
    # directorio en el cual se crearan los backups
    _backup = Path(fr"{Path(__file__).parent}\backup")

    @property
    def max_row_limit(cls) -> int:
        return cls._max_row_limit
    
    @max_row_limit.setter
    def max_row_limit(cls, value) -> None:
        if isinstance(value, int):
            if 0 <= value <= 50_000:
                cls._max_row_limit = value
            else:
                raise ValueError(f"solo se admiten valores entre 0 y 50000 para el máximo de filas pero su valor fue {value}")
        else:
            raise ValueError(f"el valor a asignar debe ser un int pero fue {type(value).__name__}")
    
    @property
    def max_col_limit(cls) -> int:
        return cls._max_col_limit
    
    @max_col_limit.setter
    def max_col_limit(cls, value) -> None:
        if isinstance(value, int):
            if 1 < value <= 20:
                cls._max_col_limit = value
            else:
                raise ValueError(f"solo se admiten valores entre 1 y 20 para el máximo de columnas pero su valor fue {value}")
        else:
            raise ValueError(f"el valor a asignar debe ser un int pero fue {type(value).__name__}")
        
    @property
    def max_name_limit(cls) -> int:
        return cls._max_name_limit
    
    @max_name_limit.setter
    def max_name_limit(cls, value) -> None:
        if isinstance(value, int):
            if 0 < value <= 150:
                cls._max_name_limit = value
            else:
                raise ValueError(f"solo se admiten valores entre 1 y 150 para el máximo de caracteres en el nombre de un archivo csv pero su valor fue {value}")
        else:
            raise ValueError(f"el valor a asignar debe ser un int pero fue {type(value).__name__}")
    
    @property
    def backup(cls) -> Path:
        return cls._backup
    
    @backup.setter
    def backup(cls, value) -> None:
        if isinstance(value, Path):
            if value.is_dir():
                cls._backup = value
            else:
                raise ValueError(f"el directorio donde se guardan los backup debe ser uno valido pero fue {value}")
        else: 
            raise ValueError(f"el valor a asignar debe ser Path pero fue {type(value).__name__}")

# se ocupan clases (dataclasses) ya que permiten definir una capa extra de verificación
# de tipos de datos de atributos
class BaseCsvManager(metaclass=Meta):
    """ clase BaseCsvManager su función es servir como clase base para manejar la
    verificación de atributos para la clase SingleCsvManager

    Argumentos de clase:

    - max_row_limit: un int que designa la cantidad máxima de filas que puede tener un archivo csv

    - max_col_limit: un int que designa la cantidad máxima de columnas que puede tener un archivo csv

    - max_name_limit: un int que designa la cantidad máxima de caracteres que puede tener el nombre de un archivo csv

    - backup: una instancia de Path que designa el directorio en el cual se crearan los backups

    para modificar el valor de cualquiera de los atributos de clase anteriores se debe hacer 
    usando BaseCsvManager.atributo = valor puesto que la implementación no permite cambiar estos 
    valores desde una instancia de la clase

    Argumentos de iniciación:

    - file_name: un str que designa el nombre que se ocupara para crear el archivo en el backup

    - current_class: una clase (no una instancia de clase) o None que se ocupara para guardar
    los datos pasados a ella usando el método set_data

    - delimiter: un str que designa el separador que se ocupara para el archivo csv (por defecto es "|")

    - exclude: None o una tuple con str que designa los atributos que se deben excluir o incluir,
    para que funcione para incluir envés de excluir el primer elemento del tuple debe ser el un "!"
    """

    def __init__(self, file_name: str, current_class: Type | None = None, 
                 delimiter: str = "|", exclude: None | tuple = None) -> None:
        self.file_name = file_name
        self.current_class = current_class
        self.delimiter = delimiter
        self.exclude = exclude
        self.writer_instance = None
        # si el usuario quiere cambiar el nombre del archivo es mejor que lo haga a
        # traves de un método
        self.instance_file_path = Path(fr"{BaseCsvManager.backup}\{self.file_name}.csv")

    @property
    def file_name(self) -> str:
        return self._file_name

    # setter para la ruta del archivo csv
    # el usuario debe introducir un str
    # que también sea una ruta existente
    # en el sistema operativo
    @file_name.setter
    def file_name(self, value) -> None:
        if isinstance(value, str):
            if re.match(fr"^[A-Za-z_0-9\-]{{1,{BaseCsvManager.max_name_limit}}}$", value) is not None:
                self._file_name = value
            else: 
                raise ValueError("el nombre a asignar para el archivo debe solo contener los siguientes caracteres: "
                                 "letras mayúsculas y minúsculas (a-z pero no ñ), números guiones bajos (_) o guiones (-) y sin espacios en blanco "
                                 f"pero su valor fue {value}")
        else:
            raise ValueError(f"el valor a asignar debe ser str pero fue {type(value).__name__}")

    @property
    def current_class(self) -> Type:
        return self._current_class
    
    @current_class.setter
    def current_class(self, value) -> None:
        # expected behavior is to pass to object
        # an object not an instance of it
        # this should cover most cases 
        # (except metaclass defined class, low level c object,
        # classes that are also callables)
        if isclass(value) or value is None:
            self._current_class = value
            self.can_save = False if "__dict__" not in dir(self.current_class) else True
        else:
            raise ValueError("El valor de class_object debe ser un objeto o None no una instancia del mismo y el uso "
                             "de este programa se limita a clases que devuelvan True al usarse la función isclass()")
    
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

    @staticmethod
    def _create_folders(file_name: Path) -> None:
        """ método estático privado _create_folders crea los directorios y archivos
        necesarios para el backup y los archivos csv que almacenara

        Argumento:

        - file_name un objeto de tipo Path que designa el archivo csv que se creara

        Valor de retorno:

        - None
        """
        directory = Path(file_name.parent)
        if not directory.is_dir():
            directory.mkdir()
        if not file_name.is_file():
            with open(str(file_name), "w", newline="", encoding="utf-8") as _:
                pass

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
    
    # siempre consultar en este método si el nuevo archivo a crear (file_name)
    # existe o no queda a decision del que use esta librería definir que pasa después
    # ya que el usuario podría esperar seguir escribiendo en un archivo anterior
    # o crear uno nuevo
    @staticmethod
    def return_current_file_names() -> Generator[str, None, None]:
        """ método estático publico return_current_file_names

        Argumentos:

        - None

        Valor de retorno:

        - un generador que permite enviar de uno en uno los nombres de los archivos
        presentes en el backup actual
        """
        for child_content in BaseCsvManager.backup.iterdir():
            if child_content.is_file() and child_content.suffix == ".csv":
                yield child_content.stem

    # para eliminar archivos existentes en el backup
    @staticmethod
    def delete_record(*args) -> None:
        """ método estático publico delete_record
        
        Argumentos:

        - args de cantidad variable los que se esperan que sean los
        nombres de los archivos a eliminar del backup presente, si entre 
        los argumentos se pasa el valor "borrar todo" se eliminaran todos los archivos

        Valor de retorno:

        - None
        """
        file_names = []
        for val in args:
            if isinstance(val, str):
                file_names.append(val)
        if file_names:
            if "borrar todo" in file_names:
                for csv_file in BaseCsvManager.return_current_file_names():
                    Path(fr"{BaseCsvManager.backup}\{csv_file}.csv").unlink(missing_ok=True)
            else:
                for present_file in BaseCsvManager.return_current_file_names():
                    if present_file in file_names:
                        Path(fr"{BaseCsvManager.backup}\{present_file}.csv").unlink(missing_ok=True)

    def rename_file(self, new_name) -> None:
        """ método público rename_file
        
        Argumento:

        - new_name el nuevo nombre que tendrá el archivo en el backup

        Valor de retorno:

        - None

        Excepciones:

        - ValueError si el argumento new_name no cumple con las restricciones
        impuestas para nombrar archivos en el backup    
        """
        # this is to call the property setter
        self.file_name = new_name
        self.instance_file_path: Path = self.instance_file_path.rename(Path(fr"{BaseCsvManager.backup}\{new_name}.csv"))

    # right way of passing data to current object
    # TEST GUARDAR_DATOS WITH WRITER_INSTANCE = NONE AND CHANGE THE CORRESPONDING TEST 
    # TO MATCH THE CURRENT WAY DATA IS PASSED TO AN OBJECT
    # TEST CURRENT OBJECT SETTER AND GETTER
    # warning or error I don't know
    # should I use a custom error here
    def set_data(self, *args, **kwargs) -> Self | None:
        """ método público set_data permite pasar datos a la clase
        actual del atributo current_class

        Argumentos:

        - args o kwargs de cantidad variable los cuales son pasados a current_class
        para su iniciación

        Valor de retorno:

        - la instancia actual de la clase si current_class no es None de lo contrario se retorna None

        Excepciones:

        - ValueError si alguno de los argumentos pasados a current_class
        no son los esperados
        """
        if self.can_save:
            try:
                self.writer_instance = self.current_class(*args, **kwargs)
            except Exception as base_error:
                raise ValueError(f"no se pudo realizar el paso de datos a la clase actual debido al siguiente error: {base_error}")
            return self
        return None

# IMPORTANTE PARA LEER Y ESCRIBIR A UN CSV QUE YA TENIA DATOS
# DEBES PASAR ESE ARCHIVO USANDO EL MÉTODO DE CLASE INDEX PRIMERO
# A SI SE COPIAN SUS DATOS AL BACKUP Y AL INICIAR LA CLASE EN FILE_NAME
# SE DEBE PASAR YA SEA ESE MISMO NOMBRE DE ARCHIVO (SI SE QUIERE QUE LOS CAMBIOS SE EFECTÚEN
# SOBRE EL MISMO) U OTRO NUEVO (DONDE SE HARÁN TODOS LOS CAMBIOS PUDIENDO ASÍ MANTENER
# LA FUENTE ORIGINAL DE LOS DATOS) Y SE DEBE PASAR EL RESULTADO DE RETORNO DE INDEX A CURRENT_CLASS DE LO CONTRARIO
# SI SE PARTE SIN DATOS Y SE QUIERE EMPEZAR A ESCRIBIR LOS DATOS DE UNA CLASE A UN ARCHIVO
# EN BLANCO PASE LA CLASE DEL OBJETO A CURRENT_CLASS

class SingleCsvManager(BaseCsvManager):
    """ clase SingleCsvManager su función es permitir realizar operaciones
    de lectura, escritura, edición y borrado de los datos en un archivo csv

    Los argumentos de iniciación son los mismos que ocupa la clase BaseCsvManager, por lo que se
    recomienda referirse a la documentación de esa clase para obtener más información

    Importante: para obtener la ruta absoluta del archivo csv que ocupa la instancia actual 
    ocupe el atributo instance_file_path de esta clase o el padre de ella
    """   

    def __init__(self, file_name: str, current_class: Type | None = None, delimiter: str = "|", 
                 exclude: None | tuple = None) -> None:
        super().__init__(file_name, current_class, delimiter, exclude)
        self._create_folders(self.instance_file_path)
        # format the current file correctly
        # so the index col is as expected
        self.current_rows: int = self.__len__()
        if self.current_rows:
            self.new_head = tuple((val.upper() for val in next(self.leer_datos_csv())))

    def guardar_datos_csv(self, enforce_unique=None) -> str:
        """ método publico guardar_datos_csv
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
        que la entrada a guardar ya estaba presente se retorna el str presente, si los datos no fueron
        pasados a la instancia (atributo current_class) apropiadamente o se supera el máximo de filas o columnas
        por archivo se retorna un str que comienza con un mensaje de advertencia como "Advertencia: "

        Excepciones:

        - ValueError si el valor del argumento no es el apropiado o
        se intenta guardar una entrada con más valores (atributos) que la cantidad de columnas
        disponibles o con nombres de valores que no sean iguales a los ya presentes (nombre columnas)
        """
        if not self.can_save:
            return (f"\nAdvertencia: Actualmente esta ocupando un objeto de tipo {type(self.current_class).__name__}"
                    "el cual no posee un __dict__ por lo que es imposible guardar entradas con él")
        if self.writer_instance is None:
            return ("\nAdvertencia: Para poder crear una nueva entrada primero debe pasar sus datos usando el método set_data "
                    "para acceder a métodos de su clase pasada o cambiar sus datos debe hacerlo a través del atributo writer_instance")   
        # self.current_rows can't be less than zero so even if self.max_row_limit
        # is negative (truthy) the firs condition still checks
        if self.current_rows - 1 >= BaseCsvManager.max_row_limit or not BaseCsvManager.max_row_limit:
            return ("\nAdvertencia: Su entrada no fue creada ya que para mantener la eficiencia de este programa "
                    f"recomendamos\nlimitar el numero de entrada a {BaseCsvManager.max_row_limit - 3_000} "
                    f"favor de ir a\n{self.instance_file_path}\nhacer una, copia reiniciar el programa y\n"
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
            vals_to_check = " | ".join([f'"{str(key).strip("_")}" = {val}' for
                                        key, val in self.writer_instance.__dict__.items() if
                                        str(key).strip("_") in enforce_unique])
            skip_first = self.leer_datos_csv(search=vals_to_check)
            next(skip_first)
            for _ in skip_first:
                return "presente"
        if self.exclude is not None:
            if self.exclude[0] == "!":
                class_repr = [(str(key).strip('_').upper(),
                                str(val)) for key, val in self.writer_instance.__dict__.items()
                                if str(key).strip("_") in self.exclude]
            else:
                class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.writer_instance.__dict__.items()
                                if str(key).strip("_") not in self.exclude]
        else:
            class_repr = [(str(key).strip('_').upper(), str(val)) for key, val in self.writer_instance.__dict__.items()]
        if len(class_repr) > BaseCsvManager.max_col_limit:
            return ("\nAdvertencia su entrada no fue creada ya que el objeto a guardar contiene un "
                    f"__dict__ que supera el máximo de columnas que puede tener un objeto ({BaseCsvManager.max_col_limit}) "
                    "puede usar el argumento exclude de esta clase para excluir algunos atributos y disminuir el número de columnas")
        if not self.current_rows:
            with open(self.instance_file_path, "a", newline="", encoding="utf-8") as csv_writer:
                write = csv.writer(csv_writer, delimiter=self.delimiter)
                if self.exclude is not None:
                    if self.exclude[0] == "!":
                        self.new_head = ["INDICE", *[str(key).strip("_").upper() for key in self.writer_instance.__dict__ if
                                            str(key).strip("_") in self.exclude]]
                        write.writerow(self.new_head)
                    else:
                        self.new_head = ["INDICE", *[str(key).strip("_").upper() for key in self.writer_instance.__dict__ if
                                            str(key).strip("_") not in self.exclude]]
                        write.writerow(self.new_head)
                else:
                    self.new_head = ["INDICE", *[str(key).strip("_").upper() for key in self.writer_instance.__dict__]]
                    write.writerow(self.new_head)
            self.current_rows += 1
        if tuple((val[0] for val in class_repr)) != tuple(self.new_head[1:]):
            raise ValueError("solo se permiten objetos "
                            "con el mismo número de atributos y nombres "
                            f"que el actual {', '.join(self.new_head[1:])}")
        with open(self.instance_file_path, "a", newline="", encoding="utf-8") as csv_writer:
            write = csv.writer(csv_writer, delimiter=self.delimiter)
            self.current_rows += 1
            write.writerow([f"[{self.current_rows - 1}]", *[val[1] for val in class_repr]])
        return (f"\n{f'{self.delimiter}'.join([*self.new_head])}\n[{self.current_rows - 1}]"
                    f"{self.delimiter}{f'{self.delimiter}'.join([val[1] for val in class_repr])}")


    def leer_datos_csv(self, search="", escaped=False, query_functions=True) -> Generator[list[str] | str, None, str]:
        """ método publico leer_datos_csv

        Argumentos:

        - search es la str que se usa para buscar dentro del csv

        - escaped es para saber si se debe escapar algún carácter especial
        en el search str cuando se realize la búsqueda mediante expresiones regulares

        - query_function es para determinar si se debe aplicar o no las funciones
        pasadas en una query

        Valor de retorno:

        - un generador que permite enviar de una en una las lineas dentro del archivo csv
        Para saber que sintaxis es ocupada para buscar por indice refiérase al método estático publico
        return_pattern (siempre el primer dato enviado sera el encabezado del csv 
        independiente de la query usada en el argumento search). Si se ocupa alguna de las siguientes funciones
        en una query (UNIQUE, MIN, MAX, SUM, AVG, COUNT) adicionalmente el ultimo item tendrá la siguiente estructura:

        1- si fue MIN, MAX, SUM, AVG: una lista donde el primer item es el nombre de la operación, el segundo es la
        columna en la cual se efectuó y el ultimo el resultado de la operación (si se ocupa en datos no numéricos es 0)

        2- si fue COUNT: una lista con dos items el primero el nombre de la operación y el segundo el total de entradas

        3- si fue UNIQUE: una lista con 4 items el primero el nombre de la operación, el segundo la columna donde
        fue aplicada, el tercero un dict de la frecuencia de cada valor y el cuarto el total
        de filas con valores únicos

        Excepciones:

        - ValueError si los tipos de los argumentos no son los apropiados
        """
        if not isinstance(search, str):
            raise ValueError(f"el argumento search debe ser un str pero fue {type(search).__name__}")
        for name, item in {"escaped": escaped, "query_functions": query_functions}.items():
            if not isinstance(item, bool):
                raise ValueError(f"el argumento {name} debe ser un bool pero fue {type(item).__name__}")
        if self.current_rows > 0:
            with open(str(self.instance_file_path), "r", newline="", encoding="utf-8") as csv_reader:
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
                    elif (list_of_match:= self.__query_parser(search)):
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
                                            function_match += [operand, set(), [0, dict()], col_index]
                                        elif operand == "ASC" or operand == "DESC":
                                            header_offset = sum((1 for item in except_col if item < col_index))
                                            function_match += [operand, [], col_index - header_offset, set()]
                            else:
                                list_of_match.pop()
                        for row in read:
                            bool_values: list | str = self.__parsed_query_operation_resolver(row, list_of_match)
                            if isinstance(bool_values, str):
                                yield bool_values
                                return "sintaxis no valida búsqueda terminada"
                            elif not bool_values:
                                yield "error de sintaxis"
                                return "sintaxis no valida búsqueda terminada"
                            else:
                                current_value = bool_values.pop(0)
                                if len(bool_values) != 1:
                                    for current in range(0, len(bool_values), 2):
                                        if bool_values[current] == "|":
                                            current_value = current_value or bool_values[current + 1]
                                        else:
                                            current_value = current_value and bool_values[current + 1]
                                if current_value:
                                    if function_match:
                                        new_function_state: str = self.__query_function_state_updater(row, except_col, function_match)
                                        if new_function_state == "REACHED-LIMIT":
                                            return ("se alcanzo el limite de entradas "
                                                    f"requeridas LIMIT:{function_match[-1]}")
                                        # this is only because this two functions should 
                                        # be able to yield rows at this point
                                        if new_function_state not in ("LIMIT", "UNIQUE"):
                                            continue
                                    if except_col:
                                        yield [row[0]] + [row[item] for item in range(1, len(row)) if
                                                        item not in except_col]
                                    else:
                                        yield row
                        if function_match:
                            if len(function_match) == 4:
                                if function_match[0] not in ("UNIQUE", "PRESENT"):
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
                                else:
                                    yield ["UNIQUE", self.new_head[function_match[-1]], function_match[2][-1], function_match[2][0]]
                            elif len(function_match) == 3:
                                if function_match[0] not in ("MIN", "MAX"):
                                    yield [function_match[0], self.new_head[function_match[-1]],
                                        function_match[1] if not isnan(function_match[1]) else 0]
                                else:
                                    yield [function_match[0], self.new_head[function_match[-1]],
                                        function_match[1][0] if function_match[1][0] != "STR" 
                                        else function_match[1][1]]
                            # LIMIT might be able to get to here in is set
                            # bigger than the total amount of entries on a search
                            elif function_match[0] == "COUNT":
                                yield ["COUNT", function_match[-1]]
                        return "búsqueda completa"
                    else:
                        yield next(read)
                        for row in read:
                            if row and re.search(f"^.*{re.escape(search) if not escaped else search}.*$",
                                                "".join(row[1:]), re.IGNORECASE) is not None:
                                yield row
                else:
                    for row in read:
                        if row:
                            yield row

    def borrar_datos(self, delete_index="") -> Generator[str, None, str]:
        """ método publico borrar_datos
        permite borrar las entradas seleccionadas del archivo csv

        Argumentos:

        - delete_index str que especifica que entradas a borrar
        los valores validos para borrar entradas son 'borrar todo', alguno de los
        patrones validos establecidos por el método estático return_pattern o una query
        valida para buscar datos que sea de estructura DELETE ON <query búsqueda>

        Valor de retorno:

        - un generador que devuelve de una en una las entradas borradas si alguna se borro
        como un str

        Excepciones:

        - ValueError si alguno de los argumentos no es del tipo esperado o si se introduce
        un formato para el argumento delete_index que no devuelva algún valor del csv
        """
        if not isinstance(delete_index, str):
            raise ValueError(f"el argumento delete_index debe ser str pero fue {type(delete_index).__name__}")
        if delete_index == "borrar todo":
            if self.current_rows <= 1:
                yield "nada"
                return "no hay datos para borrar"
            with open(self.instance_file_path, "w", newline="", encoding="utf-8") as _:
                pass
            self.current_rows = 0
            yield "todo"
            return "todos los items ya se borraron"
        else:
            if self.current_rows <= 1:
                yield "nada"
                return "no hay datos para borrar"
            # use regex to accept multiple entries to delete
            if isinstance(to_delete := self.return_pattern(delete_index), tuple):
                # to get rid of things like 00 or 03, 056
                operation: str | None = to_delete[0]
                vars_to_delete: list = [num for num in to_delete[-1] if self.current_rows >= num >= 0]
                if not vars_to_delete:
                    raise ValueError("ninguno de los valores ingresados corresponde al indice de alguna entrada")
                # we are hoping that the user does not pass a not indexed file for this to work properly
                yield f"{self.delimiter}".join([*self.new_head])
                if operation == ":":
                    if len(vars_to_delete) == 1:
                        vars_to_delete.append(self.current_rows)
                    vars_to_delete.append("range")
                else:
                    vars_to_delete = [f"[{num}]" for num in vars_to_delete]

                # copying all the data except entries to delete from backup file to main file
                # CHECK THE USE OF W+B MODE IS COMPATIBLE WITH THE USE OF CSV WRITEROW
                with tempfile.TemporaryFile(mode="w+t", encoding="utf-8", newline="", suffix=".csv") as write_filter:
                    filter_ = csv.writer(write_filter, delimiter=self.delimiter)
                    count: int = 1
                    mark: str = vars_to_delete[-1]
                    row_generator: Generator[list[str] | str, None, str] = self.leer_datos_csv()
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
                    self.__rewrite_data(write_filter)

            elif (regex_delete := re.search(r'^DELETE ON (.+?)$', delete_index)) is not None:
                delete_on = self.leer_datos_csv(search=regex_delete.group(1), query_functions=False)
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

                with tempfile.TemporaryFile(mode="w+t", encoding="utf-8", newline="", suffix=".csv") as delete_query:
                    deleter = csv.writer(delete_query, delimiter=self.delimiter)
                    reader: Generator[list[str] | str, None, str] = self.leer_datos_csv()
                    yield f"{self.delimiter}".join([*self.new_head])
                    deleter.writerow(next(reader))
                    counter: int  = 1
                    for entry in reader:
                        if entry[0] not in to_delete:
                            entry[0] = f"[{counter}]"
                            deleter.writerow(entry)
                            counter += 1
                        else:
                            yield f"{self.delimiter}".join(val for val in entry)
                    self.__rewrite_data(delete_query)
            else:
                raise ValueError("utilize uno de los siguientes formatos para borrar una entrada:\n"
                                 "[n], [n:m], [n:], [n-m-p] (hasta 10) remplazando las letras por el indice\n"
                                 "de lo que desee eliminar o escribiendo una consulta usando la palabra clave DELETE para selecciones más complejas")
            # deleting all data on backup (is not up to date)
            # synchronizing backup
            # should I use asyncio?
            self.current_rows = self.__len__()

    def actualizar_datos(self, update_query, map_values = None) -> Generator[dict | str, None, str]:
        """ método publico actualizar_datos

        Argumentos:

        - update_query es la str utilizada para determinar que columnas y a que valor
        actualizarlas y en que filas

        - map_values un dict o None si es dict debe contener como llaves el valor que se quiere actualizar y como 
        valores el nuevo valor que tendrá la entrada a actualizar, para ocupar estos valores en una UPDATE query se tiene
        que ocupar la función %MAP-VALUE la cual no acepta ningún argumento,  si se intenta ocupar %MAP-VALUE y map_values no es
        dict entonces se pasara un error al valor de retorno

        Valor de retorno:

        - un generador que retorna ya sea una str con mensaje de errores de sintaxis o que no se encontraron entradas para actualizar, o
        un diccionario por cada fila actualizada con la siguiente estructura:
        3 llaves result, errors y old. Result contiene una lista con los valores de cada fila, errors contiene un diccionario
        con los nombres de las filas a actualizar y cada nombre tiene como valor una lista la cual esta vacía si no hubo errores
        a la hora de actualizar el valor de la fila de esa columna y que contendrá un mensaje con el error si hubo problemas, 
        finalmente la llave old contiene un diccionario igual al de errors en donde cada valor es una lista de los valores anteriores
        que tenia la columna en esa fila si este fue actualizada (no hubo errores)

        ejemplos:
        
        1- si no hubo errores al actualizar una fila se obtiene un valor como el siguiente:
        {'result': ['[2]', 'Lucas Folch', 'Los Angeles', '30', 'Graphic Designer', '2024-08-29'], 
        'errors': {'NAME': [], 'AGE': []}, 
        'old': {'NAME': ['Jane Smith'], 'AGE': ['34']}}

        2- si hubo valores que no pudieron ser actualizados:
        {'result': ['[4]', 'Emily Davis', 'Houston', '-8.0', 'Marketing Specialist', '2024-08-27'], 
        'errors': {'AGE': [], 'DATE': ['superado el número de días que se puede añadir o restar a una fecha (entre 1 y 1000) ya que su valor fue 10500 entrada [4] no actualizada']}, 
        'old': {'AGE': ['8'], 'DATE': []}}

        3- si ningún valor pudo ser actualizado:
        {'result': ['[11]', 'ningún valor de la fila fue actualizado, todas la operaciones fueron invalidas'], 
        'errors': {'AGE': ['no se puede aplicar una función que no sea %ADD sobre un str y su elección fue %MUL entrada [11] no actualizada'], 'DATE': ['superado el número de días que se puede añadir o restar a una fecha (entre 1 y 1000) ya que su valor fue 10500 entrada [11] no actualizada']}, 
        'old': {'AGE': [], 'DATE': []}}


        Excepciones:

        - ValueError si los tipos de los argumentos no son los apropiados
        """
        if not self.current_rows:
            raise ValueError("no es posible actualizar si no hay valore disponibles")
        if not isinstance(update_query, str):
            raise ValueError(
                f"debe ingresar un str como instrucción para actualizar valores, pero se introdujo {type(update_query).__name__}")
        # only considere map_values if is a dict otherwise pass an error if the user
        # try to use the %MAP-VALUE function
        if map_values is not None:
            if isinstance(map_values, dict):
                map_values = {str(key): str(value) for key, value in map_values.items()}

        # this works but use .strip() on the values to update
        # current re implementation only captures a group as needed
        # that why this is as verbose as it gets
        pattern = (r'^UPDATE:~"([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?)(?: "([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?))?'
                   r'(?: "([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?))?(?: "([^,\s><=\|&!:"+*-\.\'#/\?]+"=.+?))? ON (.+?)$')
        regex_update = re.search(pattern, update_query)
        if regex_update is not None:
            value_tokens = list(filter(None, regex_update.groups()))
            where_update = value_tokens.pop()
            search_result: Generator[list[str] | str, None, str] = self.leer_datos_csv(search=where_update, query_functions=False)
            head_update: list[str] = next(search_result)
            # stores the column index to be updated and also the value or updating 
            # function the value of that col index is going to be updated to
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
            # for the case that any value was not updated due to impossible update function
            # use due to incorrect expected types for arguments
            was_updated: int = 0
            with tempfile.TemporaryFile(mode="w+t", encoding="utf-8", newline="", suffix=".csv") as write_update:
                updater = csv.writer(write_update, delimiter=self.delimiter)
                reader: Generator[list[str] | str, None, str] = self.leer_datos_csv()
                updater.writerow(next(reader))
                for count, entry in enumerate(reader, start=1):
                    if f"[{count}]" in row_index:
                        # make this the return value, if str is yield create a new key with
                        # the col name and set its value to list, you should use a list in case
                        # one col has multiple errors, by convention col names always in uppercase
                        # IF error is present you delete the last old if no old is found then that row do not suffered
                        # any changes
                        
                        # fix this because if there is a str message then the entry is going to be
                        # pass to the user even if no changed was made (all cols yielded a str) or
                        # if only some cols where updated
                        # if all entries update have errors then we do not have old values since 
                        # they are still the same and in result we let know that no update was done
                        update_status: dict[str, list | dict[str, list]] = self.__parsed_update_query_operation_resolver(entry, col_index, count, map_values)
                        if sum(len(old_column) for old_column in update_status["old"].values()):
                            update_status["result"] = entry
                            was_updated += 1
                        else:
                            update_status["result"] = [entry[0], "ningún valor de la fila fue actualizado, todas la operaciones fueron invalidas"]
                        yield update_status
                    updater.writerow(entry)
                if was_updated:
                    self.__rewrite_data(write_update)
        else:
            yield "error de sintaxis"

    def __query_parser(self, string_pattern) -> list:
        """ método privado __query_parser
        permite aplicar operaciones lógicas (>=, <=, <, >, =, != [=, ]=, [], ][, <>, ><, <<, >>, {}, }{) a las búsquedas
        del usuario dando más posibilidades al momento de filtrar datos

        Argumento:

        - string_pattern str introducida por el usuario la cual sera ocupada
        para hacer las comparaciones requeridas a la hora de buscar datos el formato
        general debe ser '"<nombre_columna>" <operador_lógico> <valor>'

        Valor de retorno:

        - una lista con los tokens a utilizar para filtrar resultados si se encontró un
        patron de otra forma un lista vacía

        Excepciones:

        - ValueError si el tipo del argumento requerido no es el indicado
        """
        if not isinstance(string_pattern, str):
            raise ValueError(
                f"el tipo del argumento string_pattern debe ser str pero fue {type(string_pattern).__name__}")       
        query_regex: str = (r'^(?:!?\[([^\[,\s><=\|&!:"+*-\.\'/\?\]]+)*\] )?"([^\s,><=\|&!":+*-\.\'#/\?\[\]]+)" '
                            r'(>=|>|<=|<|=|!=|\[=|\]=|\[\]|\]\[|<>|><|<<|>>|\{\}|\}\{) (.+?)')
        query_regex += r"(?: (\||&) "
        query_regex += r"(?: (\||&) ".join([r'"([^,\s><=\|&!:"+*-\.\'#/\?\[\]]+)" (>=|>|<=|<|=|!=|\[=|\]=|\[\]|\]\[|<>|><|<<|>>|\{\}\}\{) (.+?))?'
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
                if exclude in ("<=", ">=", ">", "<", "=", "!=", "[=", "]=", "[]", "][", "<>", "><", "<<", ">>", "{}", "}{"):
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

    def __parsed_query_operation_resolver(self, current_row: list, parsed_query: list) -> list | str:
        """ método privado parsed_query_operation_resolver implementa el código
        que permite tomar la parte de una query que aplica las comparaciones lógicas
        y retornar una lista con el resultado de cada comparación en un query ya sea True o False

        Argumentos:

        - current_row una lista con los valores de la fila que esta siendo actualmente leída

        - parsed_query una lista que contiene datos extraídos de la query como operación a realizar y
        valor con el cual comparar

        Valor de retorno:

        - un str si se detecto un error de sintaxis en los valores entregados por parsed_query, de lo 
        contrario una lista que contiene el resultado de las comparaciones lógicas pasadas en la query
        """
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
        "[]": lambda x, y: str(y).lower() in str(x).lower(),
        "][": lambda x, y: str(y).lower() not in str(x).lower(), 
        "<>": lambda x, y: len(x) == y, "><": lambda x, y: len(x) != y,
        ">>": lambda x, y: len(x) > y, "<<": lambda x, y: len(x) < y,
        "{}": lambda x, y: x in y, "}{": lambda x, y: x not in y,
        }
        for element in parsed_query:
            if isinstance(element, list):
                try:
                    head_index = self.new_head.index(element[0])
                except IndexError:
                    # error if col that logical operator is applied to 
                    # is not a valid col name (does not exist)
                    return "error de sintaxis"
                # so you can search from index to
                if head_index > 0:
                    val = current_row[head_index]
                else:
                    # for suing [= ]= with index you don't have to consider []
                    val = re.sub(r"[\[\]]", "", current_row[head_index])
                # should always be present on dict no need for get
                parse_function: Callable = operation_hash_table[element[1]] 
                if element[1] in ("]=", "[=", "[]", "]["):
                    # this operator only accept values as str no matter if both can
                    # be numbers of dates otherwise when parsing to float the comparison
                    # may be False when is True
                    # since both are string for this case
                    # no error should be expected to be raised
                    bool_values.append(parse_function(val, element[-1]))
                elif element[1] in ("{}", "}{"):
                    if (match_group := re.match(r"^%RANGE:(.)\[(.+)\]$", element[-1])) is not None:
                        range_values: list = filter(None, match_group.group(2).split(match_group.group(1)))
                        bool_values.append(parse_function(val, range_values))
                    else:
                        return f"error de sintaxis al ocupar el operador {element[1]}"
                elif element[1] in ("<>", "><", "<<", ">>",):
                    try:
                        bool_values.append(parse_function(str(val), int(element[-1])))
                    except ValueError:
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
        return bool_values

    def __query_function_state_updater(self, current_row: list, query_headers: tuple, status_container: list) -> str:
        """ método privado query_function_state_updater gestiona la actualización del estado actual
        del objeto en este caso una lista encargado de almacenar el valor acumulado para la función pasada
        en la query

        Argumentos:

        - current_row una lista que contiene los valores de la fila actual que se lee del archivo csv

        - query_headers un tuple que contiene el formato actual del header especificado en la query

        - status_container una lista que almacena distintas variables dependiendo de la función usada en el query

        Valor de retorno:

        - un str que retorna el identificador de la función usada actualmente o el estado actual de esa
        función (LIMIT o REACHED-LIMIT, UNIQUE o PRESENT)
        """
        if status_container[0] == "LIMIT":
            status_container[-1] -= 1
            if status_container[-1] == 0:
                return "REACHED-LIMIT"
        elif status_container[0] == "COUNT":
            status_container[-1] += 1
        elif status_container[0] in ("UNIQUE", "PRESENT"):
            before_len = len(status_container[1])
            current_value = current_row[status_container[-1]]
            status_container[1].add(current_value)
            # if the item was not present is unique
            # otherwise it was there already
            if before_len != len(status_container[1]):
                status_container[0] = "UNIQUE"
                # updating the count of unique values
                status_container[2][0] += 1
            else:
                status_container[0] = "PRESENT"
            # adding the values to a dict to keep its
            # frequency
            if current_value not in status_container[2][-1]:
                status_container[2][-1][current_value] = 1
            else:
                status_container[2][-1][current_value] += 1
        elif status_container[0] in ("ASC", "DESC"):
            last_len = len(status_container[-1])
            current_row = [current_row[0],] + [current_row[item] for item in
                                        range(1, len(current_row)) if
                                        item not in query_headers] if query_headers else current_row
            status_container[-1].add(current_row[status_container[2]])
            if last_len != len(status_container[-1]):
                if query_headers:
                    status_container[1].append(current_row)
                else:
                    status_container[1].append(current_row)
        else:
            function_val = current_row[status_container[-1]]
            for is_type in (float, date.fromisoformat, str):
                try:
                    val_type = is_type(function_val)
                except ValueError:
                    pass
                else:
                    if (status_container[0] == "AVG" and not 
                            isnan(status_container[1])):
                        try:
                            status_container[1] += val_type
                            status_container[2] += 1
                        except TypeError:
                            status_container[1] = float("nan")
                            status_container[2] = 0
                    elif status_container[0] == "MAX":
                        if (status_container[1][0] == float("-inf") 
                                and isinstance(val_type, date)):
                            status_container[1][0] = val_type
                        else:
                            if status_container[1][0] != "STR":
                                try:
                                    if val_type > status_container[1][0]:
                                        status_container[1][0] = val_type
                                except TypeError:
                                    status_container[1][0] = "STR"
                                if status_container[1][1] is not None:
                                    if function_val > status_container[1][1]:
                                        status_container[1][1] = function_val
                                else:
                                    status_container[1][1] = function_val
                            else:
                                if function_val > status_container[1][1]:
                                    status_container[1][1] = function_val
                    elif status_container[0] == "MIN":
                        if (status_container[1][0] == float("inf") 
                                and isinstance(val_type, date)):
                            status_container[1][0] = val_type
                        else:
                            if status_container[1][0] != "STR":
                                try:
                                    if val_type < status_container[1][0]:
                                        status_container[1][0] = val_type
                                except TypeError:
                                    status_container[1][0] = "STR"
                                if status_container[1][1] is not None:
                                    if function_val < status_container[1][1]:
                                        status_container[1][1] = function_val
                                else:
                                    status_container[1][1] = function_val
                            else:
                                if function_val < status_container[1][1]:
                                    status_container[1][1] = function_val
                    elif (status_container[0] == "SUM" and not 
                            isnan(status_container[1])): 
                        try:
                            status_container[1] += val_type
                        except TypeError:
                            status_container[1] = float("nan")
                    break
        return status_container[0]
    
    def __parsed_update_query_operation_resolver(self, update_row: list, update_value: list, current_row: int, map_values: None | dict = None) -> dict[str, list | dict[str, list]]:
        """ método privado parsed_query_operation_resolver implementa la lógica que actualiza los datos
        en una fila cuando se usa una query de actualización de valores

        Argumentos:

        - update_row una lista con los valores de la fila que va a ser actualizada

        - update_value una lista que contiene el valor con o sin funciones de actualización extraídos
        desde la query
        
        - current_row un número, el valor del indice de la fila actual

        - map_values None o un dict que contiene pares de llaves y valores que se usaran para
        actualizar los campos si se usa la función %MAP-VALUES

        Valor de retorno:

        - un diccionario que contiene información sobre el estado de la fila después de ser actualizada,
        que valores fueron cambiados (con el valor anterior) y que errores si hubo durante la actualización
        """
        update_functions: dict[str, Callable] = {
                "%UPPER": lambda x: str(x).upper(), "%LOWER": lambda x: str(x).lower(), 
                "%TITLE": lambda x: str(x).title(), "%CAPITALIZE": lambda x: str(x).capitalize(),
                "%MAP-VALUE": lambda x, y: x if not isinstance(y, dict) else y.get(x, x),
                "%REPLACE": lambda x, old, new: str(x).replace(old, new) if new != "%VOID" else str(x).replace(old, ""),
                "%ADD": lambda x, y: str(x + y), "%SUB": lambda x, y: str(x - y),
                "%MUL": lambda x, y: str(x * y), "%DIV": lambda x, y: str(x) if not y else str(x/y),
                "%RANDOM-INT": lambda x, y: str(randint(x, y)) if x < y else str(randint(y, x)),
                "%CEIL": lambda x: str(ceil(x)), "%FLOOR": lambda x: str(floor(x)),
                "%NUM-FORMAT": lambda x, y: f"{x:.{y}f}",}
            
        operations_status: dict[str, list | dict[str, list]] = {"result": [], "errors": {}, "old": {}}
        for position, new_val in update_value:
            # start appending the old value and remove it if an error is appended
            if operations_status["old"].get(self.new_head[position], None) is None:
                operations_status["old"][self.new_head[position]] = [update_row[position],]
            else:
                operations_status["old"][self.new_head[position]].append(update_row[position])
            if operations_status["errors"].get(self.new_head[position], None) is None:
                operations_status["errors"][self.new_head[position]] = []
            if new_val in ("%UPPER", "%LOWER", "%TITLE", "%CAPITALIZE"):
                update_row[position] = update_functions[new_val](update_row[position])
            elif new_val == "%MAP-VALUE":
                if isinstance(map_values, dict):
                    update_row[position] = update_functions[new_val](update_row[position], map_values)
                else:
                    operations_status["old"][self.new_head[position]].pop()
                    operations_status["errors"][self.new_head[position]].append(f"para poder aplicar la función {new_val} map_values debe ser un dict con los valores a ocupar pero fue {type(map_values).__name__}")
            # NAN AND INF DO NOT PASS THIS TRY THEY RAISE VALUE ERROR
            elif new_val in ("%CEIL", "%FLOOR"):
                try:
                    update_row[position] = update_functions[new_val](float(update_row[position]))
                except ValueError:
                    operations_status["old"][self.new_head[position]].pop()
                    operations_status["errors"][self.new_head[position]].append(f"solo es posible aplicar la función {new_val} a números y su valor fue {update_row[position]}")
            elif isinstance(new_val, list):
                if new_val[0] in ("%REPLACE",):
                    update_row[position] = update_functions[new_val[0]](update_row[position], *new_val[1:])
                elif new_val[0] == "%RANDOM-INT":
                    try:
                        limit_1 = int(new_val[1])
                        limit_2 = int(new_val[-1])
                    except ValueError:
                        operations_status["old"][self.new_head[position]].pop()
                        operations_status["errors"][self.new_head[position]].append(("para usar la función %RANDOM-INT debe pasar dos números enteros como limites inferior "
                                                                                    f"y superior pero introdujo {new_val[1]} y {new_val[-1]} entrada [{current_row}] no actualizada"))
                    else:
                        update_row[position] = update_functions["%RANDOM-INT"](limit_1, limit_2)
                elif new_val[0] == "%NUM-FORMAT":
                    parse_values: list = []
                    for count, callable_item, argument_item in ((0, float, update_row[position]), (1, int, new_val[-1])):
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
                            update_row[position] = update_functions[new_val[0]](*parse_values)
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
                            value_for_col: str = update_row[self.new_head.index(other_col_val)]
                        else:
                            operations_status["old"][self.new_head[position]].pop()
                            operations_status["errors"][self.new_head[position]].append(("el selector USE:~ solo se puede usar para pasar como argumento el valor de otra columna en la fila a la función "
                                                                                f"seleccionada por lo que el valor debe ser el nombre de una columna ({self.new_head[1:]}) pero fue {other_col_val}"))
                            continue
                    for possibly_type in ([float, float], [date.fromisoformat, int], [str, str]):
                        try:
                            cast_current_value: float | date | str = possibly_type[0](update_row[position])
                            cast_function_value: float | int | str = possibly_type[1](value_for_col)
                        except ValueError:
                            pass
                        else:
                            if isinstance(cast_current_value, float):
                                # nan and inf operations are defined internally for float
                                update_row[position] = update_functions[new_val[0]](cast_current_value, cast_function_value)
                            elif isinstance(cast_current_value, date):
                                if new_val[0] in ("%ADD", "%SUB"):
                                    if 0 < cast_function_value <= 1_000:
                                        update_row[position] = update_functions[new_val[0]](cast_current_value, timedelta(days=cast_function_value))
                                    else:
                                        operations_status["old"][self.new_head[position]].pop()
                                        operations_status["errors"][self.new_head[position]].append(("superado el número de días que se puede añadir o restar a una fecha "
                                                                                                        f"(entre 1 y 1000) ya que su valor fue {cast_function_value} entrada [{current_row}] no actualizada"))
                                else:
                                    operations_status["old"][self.new_head[position]].pop()
                                    operations_status["errors"][self.new_head[position]].append(("solo es posible aplicar una función %ADD o %SUB sobre una fecha "
                                                                                                    f"y su elección fue {new_val[0]} entrada [{current_row}] no actualizada"))
                            elif isinstance(cast_current_value, str):
                                if new_val[0] == "%ADD":
                                    update_row[position] = update_functions[new_val[0]](cast_current_value, cast_function_value)
                                else:
                                    operations_status["old"][self.new_head[position]].pop()
                                    operations_status["errors"][self.new_head[position]].append(("no se puede aplicar una función que no sea %ADD sobre un str "
                                                                                                f"y su elección fue {new_val[0]} entrada [{current_row}] no actualizada"))
                            break
            # using regex is better for case insensitive col name reference handling
            elif (copy_value := re.search(r"^%COPY:~(.+)$", new_val)) is not None:
                if (target_copy := copy_value.group(1).upper()) in self.new_head:
                    if target_copy == "INDICE":
                        update_row[position] = re.sub(r"[\[\]]", "", update_row[self.new_head.index(target_copy)])
                    else:
                        update_row[position] = update_row[self.new_head.index(target_copy)]
                else:
                    operations_status["old"][self.new_head[position]].pop()
                    operations_status["errors"][self.new_head[position]].append(("la función %COPY solo se puede usar para copiar el valor de una columna a otra para lo cual "
                                                                                f"debe seleccionar el nombre de una columna, su valor fue {target_copy} pero las opciones son {self.new_head[1:]} "
                                                                                f"entrada [{current_row}] no actualizada"))
            else:
                update_row[position] = new_val
        return operations_status
    
    def __rewrite_data(self, file_helper: TextIO) -> None:
        """ método privado rewrite_data permite reescribir los datos al
        archivo correspondiente desde un archivo temporal

        Argumentos:

        - file_helper un objeto TextIO el cual permite leer o escribir datos a un archivo en forma de texto

        Valor de retorno:

        - None
        """
        # set cursor back to the top of file
        file_helper.seek(0)
        with open(str(self.instance_file_path), "w", newline="", encoding="utf-8") as write_back:
            back_data = csv.writer(write_back, delimiter=self.delimiter)
            reader_filter = csv.reader(file_helper, delimiter=self.delimiter)
            for entry in reader_filter:
                back_data.writerow(entry)

    # you can't pass an unpacked dict to
    # a function with *args because when using args
    # the arguments become only positional and not referable by
    # names unlike normal function definition, so to stop ambiguities
    # where is difficult to know whether something is a positional or a named
    # argument you can't pass kwargs to a function that defines its arguments with *args
    @staticmethod
    def create_writer(*args) -> Type:
        """ método estático create writer
        permite crear una clase para escribir entradas en csv ya indexados pero
        que no tienen una clase de python asociada a ellos su principal función es evitar tener
        que ocupar el método de clase index si solo se quiere obtener un objeto con el cual se pueda
        guardar entradas

        Argumentos:

        - args de cantidad variable los cuales se ocuparan para crear el nombre de los atributos de la
        clase (encabezados del csv sin incluir columna INDICE si ya esta presente)

        Valor de retorno:

        - un objeto creado de tipo CsvObjectWriter el cual puede ser usado para
        escribir o actualizar a un csv que no este asociado a una clase de python

        Excepciones:

        - ValueError si el encabezado del archivo tiene nombres de columna que no son nombres de
        atributos validos en python para crear el objet
        """
        valid_headers = [not str(val).isidentifier() or iskeyword(str(val)) for val in args]
        if any(valid_headers):
            raise ValueError("el encabezado del archivo contiene caracteres inválidos "
                            f"para crear variables validas en python ({[args[id] for id in range(0,len(valid_headers)) if valid_headers[id]]})")
        
        return make_dataclass(cls_name="CsvObjectWriter", fields=[str(name).lower() for name in args])
        

    @classmethod
    def index(cls, file_path, delimiter, id_present=True, new_name = None, extra_columns = None, exclude = None) -> Type:
        """ método de clase index
        permite agregar indices con el formato de este programa y
        crear un objeto para que pueda usarse de intermediario para
        agregar o actualizar las entradas del archivo, siempre
        asume que el archivo tendrán un encabezado de otra forma los
        resultados serán inesperados, una ves reescrito el archivo este
        sera copiado al backup del programa, para crear
        el objeto que permite añadir y actualizar campos se obtienen los nombres
        de atributos desde el encabezado del archivo pero si este no es valido
        (tiene cualquier carácter que no pueda ser usado como nombre de variable o atributo 
        en python) entonces no se podrá crear el objeto y un error es generado

        Argumentos:

        - file_path: ruta valida del archivo a leer para dar formato valido

        - delimiter: el delimitador del archivo csv actual

        - id_present: bool el cual indica si se debe reescribir los id o
        si hay que crearlos desde cero

        - new_name: str o None si es str es el nuevo nombre que se le dará al archivo
        una vez creado en el backup, si es None se ocupa el nombre del archivo pasado en file_path

        - extra_columns: dict con el nombre de las columnas a añadir al archivo y su valor por defecto o None

        - exclude: list con el nombre de las columnas a excluir del archivo o None

        Valor de retorno:

        - un objeto creado de tipo CsvObjectWriter el cual puede ser usado para
        escribir o actualizar a un csv que no este asociado a una clase de python

        Excepciones:

        - ValueError si algún tipo de los argumentos ingresados no fue el correcto,
        si el encabezado del archivo tiene nombres de columna que no son nombres de
        atributos validos en python para crear el objeto o si el nombre que se le da al archivo contiene
        caracteres no validos
        """
        if not isinstance(file_path, str):
            raise ValueError(f"el valor a asignar para el argumento file_name debe ser str pero fue {type(file_path).__name__}")
        if not isinstance(id_present, bool):
            raise ValueError(
                f"el tipo esperado para el argumento id_present es bool pero fue {type(id_present).__name__}")
        if new_name is not None:
            if not isinstance(new_name, str):
                raise ValueError(f"el valor a asignar para el argumento new_name debe ser str pero fue {type(new_name).__name__}")

        new_cols = []
        default_vals = []
        excluded = []
        if extra_columns is not None:
            if isinstance(extra_columns, dict):
                # prevent empty headers
                new_cols = [dict_key for key in extra_columns.keys() if (dict_key := str(key).upper()) != "INDICE" and dict_key]
                default_vals = [str(value) if str(value) not in (" ", "") else "VOID" for value in extra_columns.values()]
            else:
                raise ValueError("si quiere añadir nuevas columnas debe pasar un argumento de tipo dict en extra_columns donde las llaves"
                                 "sean el nombre de la columna y el valor el valor por defecto que tendrá cada "
                                 f"fila para esa columna pero su argumento fue de tipo {type(extra_columns).__name__}")
        if exclude is not None:
            if isinstance(exclude, list):
                # exclude does not exclude the INDICE
                excluded.extend(set(except_index for val in exclude if (except_index := str(val).upper()) != "INDICE"))
            else:
                raise ValueError("para excluir columnas existentes debe pasar un objeto de tipo list al argumento exclude que "
                                 f"contenga el nombre de las columnas a pasar pero su argumento fue de tipo {type(exclude).__name__}")
        # FOR EMPTY CSV FILE
        # you can't get the len by this method since is not going to give back the len
        # of the current file but the one already present in the backup
        # we use new_class mostly for file_name validation
        current_file = Path(file_path)
        file_name = current_file.stem if new_name is None else new_name
        if not current_file.is_file():
            raise ValueError(f"el argumento file_name debe ser una ruta valida pero no lo fue {file_name}")
        if current_file.suffix != ".csv":
            raise ValueError(f"el argumento file_name debe ser un archivo de extension.csv pero fue {current_file.suffix}")
        if file_name in [name for name in BaseCsvManager.return_current_file_names()]:
            raise ValueError(f"el nombre ({file_name}) ocupado para crear el archivo ya existe en el backup "
                             "debe especificar otro usando el argumento new_name")
        # new argument to rename file on creation
        new_class = BaseCsvManager(file_name, None, delimiter)
        # creating backup file if does not exist (can happen if you enter a file
        # via this method before creating an instance of SingleCsvManager
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
                new_head: list[str] = [col_names for item in file_header[1:] if (col_names := item.upper()) not in excluded]
                head_file: list[str] = ["INDICE",] + file_header[1:]
            else:
                new_head: list[str] = [col_names for item in file_header if (col_names := item.upper()) not in excluded]
                head_file: list[str] = ["INDICE",] + file_header
            excluded_values = [head_file.index(col) for col in head_file if col.upper() in excluded]
            head_file: list[str] = ["INDICE",] + new_head
            # you need to calculate the index before adding new cols to the head
            # if id_present == False the INDICE becomes a reserved col name and valueError is raised
            # you can create a file with more columns than the limit by this method but ou wont be able 
            # to write to it
            head_file.extend(new_cols)
            if len(head_file) != len(set(head_file)):
                raise ValueError(f"los encabezados no pueden tener nombres repetidos para las columnas ({head_file})")
            # to not exclude index
            if 0 in excluded_values:
                excluded_values = [val for val in excluded_values if val != 0]
            cls._create_folders(new_class.instance_file_path)
            with open(new_class.instance_file_path, "w", newline="", encoding="utf-8") as write_backup:
                new_back_up = csv.writer(write_backup, delimiter=new_class.delimiter)
                new_back_up.writerow(head_file)
                for count, line in enumerate(new_import, 1):
                    if count > BaseCsvManager.max_row_limit:
                        break
                    if id_present:
                        line[0] = f"[{count}]"
                        line = [value for value in line if line.index(value) not in excluded_values]
                        new_back_up.writerow(line + default_vals)
                    else:
                        # you might end with a mismatch between the len of the headers
                        # and the entries per row if you use id_present = False
                        line = [f"[{count}]",] + line
                        new_back_up.writerow([value for value in line if line.index(value) not in excluded_values] + default_vals)
            return cls.create_writer(*head_file[1:])

    def __len__(self) -> int:
        with open(str(self.instance_file_path), "r", newline="", encoding="utf-8") as csv_reader:
            read = csv.reader(csv_reader, delimiter=self.delimiter)
            # clever method to get the len and avoid putting
            # all the contents of the file in memory
            # this 1 for _ in read is an iterator
            return sum(1 for _ in read)
