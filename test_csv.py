from saveclass import BaseCsvManager, SingleCsvManager
from pathlib import Path
from time import sleep
import shutil
import csv
import dataclasses

try:
    # use pytest <file_to_test>.py to run 
    import pytest
except ImportError:
    raise ImportError("para ejecutar este test necesita instalar "
                      "el paquete pytest, use pip install pytest "
                      "para instalarlo, pagina paquete https://docs.pytest.org/en/stable/index.html")


# IMPORTANT this are the name attributes you should refer to
# if you want to change the attribute value on an instance
# file_name, current_class, delimiter and exclude

class TestBackUpIntegrity:
    """Engloba los test de creación y el mantenimiento de la persistencia de
    datos de los archivos csv usados como backup por el programa"""
    back_up = Path(fr"{Path(__file__).parent}\backup\dir_test.csv")

    def test_creates_backup_directory(self):
        """Verificación de que si el directorio y los archivos de backup
        no existen que sean creados"""
        if (back_dir := Path(self.back_up.parent)).is_dir():
            shutil.rmtree(str(back_dir))
            sleep(2)
        SingleCsvManager(file_name="dir_test")
        sleep(1)
        assert self.back_up.is_file(), f"el archivo {self.back_up.name} no fue creado"


class TestAttribute:
    """contiene los test para comprobar que solo se acepten los valores
    correctos a la hora de inicializar la clase BaseCsvManager o a la hora
    de cambiar sus atributos de clase"""

    class AttrTesting:
        pass

    # class object can be None is th only exception where it can be an instance instead of a class
    class_object = (1, [], {}, True, 5.7, "instance", AttrTesting())
    col_sep_ = 0, "er"
    headers = (False, 
               ((), ("index",), ("index", "class"), ("index", "class", "attr", "other")), 
               ("index", "class", 3), (("INICIO", "inicio", "valor"), ("col", "col", "col")),)
    excludes = 15, (), ("wer", ["3"])

    def test_rows_range(self):
        """ test para asegurar que no se pase un argumento de tipo o valor
        incorrecto cuando se quiera cambiar el número de filas máximas por archivo"""
        # test for incorrect type arguments
        # remember that bool is a subclass of int
        for val_type in ["s", [], {}, 12.78, 15.0]:
            with pytest.raises(ValueError, match=f"debe ser un int pero fue {type(val_type).__name__}"):
                BaseCsvManager.max_row_limit = val_type

        # test for values out of range
        for val in [-14, -1, 50_001, 100_000]:
            with pytest.raises(ValueError, match=f"pero su valor fue {val}"):
                BaseCsvManager.max_row_limit = val

    def test_col_range(self):
        """ test para asegurar que no se pase un argumento de tipo o valor
        incorrecto cuando se quiera cambiar el número de columnas máximas por archivo"""
        # test for incorrect type arguments
        for val_type in ["P", (), set(), float("-inf"), float("nan")]:
            with pytest.raises(ValueError, match=f"debe ser un int pero fue {type(val_type).__name__}"):
                BaseCsvManager.max_col_limit = val_type

        # test for values out of range
        for val in [-150, 0, 26, 100]:
            with pytest.raises(ValueError, match=f"pero su valor fue {val}"):
                BaseCsvManager.max_col_limit = val

    def test_invalid_backup_dir(self):
        # no se recomienda cambiar el directorio si ya hay uno creado 
        # con otros csv ya que no se podrá acceder a ellos a menos 
        # que se vuelva al directorio anterior
        """ chequea que no se pueda pasar un directorio que no existe si
        se desea cambiar la ubicación donde se crean los backups"""
        for val_type in [False, (), set(), 12, "my_path"]:
            with pytest.raises(ValueError, match=f"debe ser Path pero fue {type(val_type).__name__}"):
                BaseCsvManager.backup = val_type

        # test for non existent directory
        with pytest.raises(ValueError, match="debe ser uno valido"):
            BaseCsvManager.backup = Path(fr"{Path(__file__).parent}\mis_backups")
    
    def test_invalid_filename_type(self):
        """ chequea que el argumento pasado sea del tipo correcto"""
        for invalid in [True, 12, {}, [], self.AttrTesting()]:
            with pytest.raises(ValueError, match="debe ser str pero fue"):
                BaseCsvManager(file_name=invalid)

    def test_invalid_filename_name(self):
        """ chequea que el nombre que se va usar para el archivo
        en el backup tenga los caracteres validos"""
        for invalid in ["file_test.csv", "#ARCHIVO", "csv respaldo", "nombre_valido_pero_muy_largo"]:
            with pytest.raises(ValueError, match=r"letras mayúsculas y minúsculas \(a-z pero no ñ\)"):
                BaseCsvManager(file_name=invalid)

    def test_invalid_object(self):
        """ chequea que si se pasa una instancia envés de una clase se genere un error"""
        for invalid in self.class_object:
            with pytest.raises(ValueError, match="debe ser un objeto (o None)"):
                BaseCsvManager("dir_test", invalid)

    def test_invalid_delimiter_type(self):
        """chequeando que si el tipo requerido para el atributo delimiter no es str
        se lance un error"""
        with pytest.raises(ValueError, match="debe ser str"):
            BaseCsvManager("dir_test", None, self.col_sep_[0])

    def test_invalid_delimiter_len(self):
        """chequeando que si el atributo delimiter contiene más de dos caracteres
        se lance un error"""
        with pytest.raises(ValueError, match="contener solo un carácter"):
            BaseCsvManager("dir_test", None, self.col_sep_[1])

    def test_invalid_exclude_type(self):
        """chequeando que si el tipo del atributo exclude no es una tuple
        se lance un error"""
        with pytest.raises(ValueError, match="ser una tuple o None"):
            BaseCsvManager("dir_test", None, "-", self.excludes[0])

    def test_invalid_exclude_len(self):
        """chequeando que si el atributo exclude es una tuple vacía
        se lance un error"""
        with pytest.raises(ValueError, match="al menos un str"):
            BaseCsvManager("dir_test", None, "-", self.excludes[1])

    def test_invalid_exclude_content_type(self):
        """chequeando que si el atributo exclude contiene no solo str
        se lance un error"""
        with pytest.raises(ValueError, match="todos los valores de la tuple"):
            BaseCsvManager("dir_test", None, "-", self.excludes[2])


class TestCsvClassSave:
    """ Engloba los test del programa cuando se usa la clase SingleCsvManager"""

    # to test object with dict but
    # no matching values for current
    # entry formatting
    # also to test exclude and
    # enforce unique
    @dataclasses.dataclass
    class SingleObjectTest:
        start: str
        end: str
        start_vals: int
        solving_time: float
        difficulty: int
        size: int
        other: str

    @dataclasses.dataclass
    class DateObjectTest:
        name: str
        city: str
        age: str
        job: str
        date: str

    @dataclasses.dataclass
    class MovieObjectTest:
        entry: int
        release: int
        title: str
        income: int

    arguments = {"start": "-432-3---1-442-3",
                 "end": "1432234131244213",
                 "start_vals": 9,
                 "solving_time": 0.0,
                 "difficulty": 8,
                 "size": 4,
                 "other": "nada"
                 }
    # lista de archivos que se ocupan como datos o casos para realizar los test
    case_data_single = Path(fr"{Path(__file__).parent}\data\data_single.csv")
    case_time_data = Path(fr"{Path(__file__).parent}\data\times.csv")
    case_empty_file = Path(fr"{Path(__file__).parent}\data\empty.csv")
    case_invalid_name = Path(fr"{Path(__file__).parent}\data\invalid name.csv")
    case_different_index_name = Path(fr"{Path(__file__).parent}\data\movies.csv")
    case_misplaced_index_col = Path(fr"{Path(__file__).parent}\data\species.csv")

    def test_static_pattern(self):
        """ comprueba que el método estático return_pattern retorne los resultados
        esperados (comportamiento no difiere en modo multi)"""
        for case in (True, 12, {"a": 5}, ["no", ], ("no",), set()):
            with pytest.raises(ValueError, match="debe ingresar un str"):
                BaseCsvManager.return_pattern(case)
        assert BaseCsvManager.return_pattern("[:3]") is None
        assert BaseCsvManager.return_pattern("[3") is None
        assert BaseCsvManager.return_pattern("palabra") is None
        assert BaseCsvManager.return_pattern("[1-6-7-8-5-11-12-67-4-3-14]") is None
        assert BaseCsvManager.return_pattern("[3:11]") == (":", [3, 11])
        assert BaseCsvManager.return_pattern("[3:]") == (":", [3, ])
        assert BaseCsvManager.return_pattern("[11:7]") == (":", [7, 11])
        assert BaseCsvManager.return_pattern("[0:0]") == (":", [0, 0])
        assert BaseCsvManager.return_pattern("[1-4-6]") == ("-", [1, 4, 6])
        assert BaseCsvManager.return_pattern("[3-8-9-17-2]") == ("-", [2, 3, 8, 9, 17])
        assert BaseCsvManager.return_pattern("[0-6-8]") == ("-", [0, 6, 8])
        assert BaseCsvManager.return_pattern("[1-6-7-8-5-11-12-67-4-3]") == ("-", [1, 3, 4, 5, 6, 7, 8, 11, 12, 67])
        assert BaseCsvManager.return_pattern("[0]") == (None, [0])
        assert BaseCsvManager.return_pattern("[12]") == (None, [12])

    def test_set_data(self):
        """ chequea que el comportamiento de test data sea el apropiado"""
        new_instance = SingleCsvManager("dir_test", None)
        assert new_instance.set_data() is None
        new_instance.current_class = self.DateObjectTest
        assert type(new_instance.set_data(*["Julia", "Arica", "40", "CIO", "15-11-1967"])).__name__ == "SingleCsvManager"
        with pytest.raises(ValueError, match="clase actual debido al siguiente error"):
            new_instance.set_data(*["Julia", "Arica", "40", "CIO"])

    def test_class_method_index(self):
        """ valida los argumentos del método de clase index para la
        importación de documentos csv"""
        # test for when you pass an empty .csv
        # index does expects a file path instead of a file name
        with pytest.raises(ValueError, match="No es posible realizar la operación en un archivo sin contenidos"):
            SingleCsvManager.index(file_path=str(self.case_empty_file), delimiter="#")
        for invalid_arg, message in (((rf"{Path(__file__).parent}\file_test.txt", "#", True), "de extension"),
                                     ((None, "d", False), "debe ser str"),
                                     ((rf"{self.case_data_single.parent}\test.csv", "<", True), "una ruta valida"),
                                     ((str(self.case_invalid_name), "#"), "debe solo contener los siguientes"),
                                     ((str(self.case_data_single), "%", "no"), "argumento id_present"),
                                     ((str(self.case_data_single), "$$", False), "debe contener solo un"),
                                     ((str(self.case_data_single), "$", False, "CSV RESPALDO TIME"), "debe solo contener los siguientes"),
                                     ((str(self.case_data_single), "$", False, "CSV_RESPALDO_TIME", set()), "si quiere añadir nuevas columnas debe pasar"),
                                     ((str(self.case_data_single), "$", False, "CSV_RESPALDO_TIME", {}, {}), "para excluir columnas existentes debe pasar"),):
            with pytest.raises(ValueError, match=message):
                SingleCsvManager.index(*invalid_arg)
        # probando que la importación tenga el formato deseado
        expected_head = ["INDICE", "NAME", "CITY", "AGE", "JOB", "DATE"]
        expected_vals = [f"[{i}]" for i in range(1, 12)]
        # this should create a backup\times.csv file
        SingleCsvManager.index(str(self.case_time_data), "#", False)
        get_values = []
        with open(fr"{Path(__file__).parent}\backup\times.csv", "r", encoding="utf-8",
                  newline="") as class_method:
            csv_read = csv.reader(class_method, delimiter="#")
            assert expected_head == next(csv_read)
            for row in csv_read:
                get_values.append(row[0])
        assert get_values == expected_vals

    def test_seek(self):
        """ comprueba que las búsquedas realizadas retorne los resultados 
        esperados"""
        # this as a class variable gave a weird behavior
        # managing class state is painful in test
        # to create backup directory
        # this creates a new file in backup
        SingleCsvManager.index(str(self.case_data_single), "#", new_name="single_seek")
        # here case data single is already in the backup and is
        # going to get write to data_test on init due to the _hash function

        single_test_instance = SingleCsvManager("single_seek", delimiter="#")
        for invalid in (8, ["test"], False, {1, 7, 9}, {"test": "no valido"}):
            with pytest.raises(ValueError, match="argumento string_pattern debe ser str"):
                single_test_instance._SingleCsvManager__query_parser(string_pattern=invalid)
        for bad_type in ((True, "test", False), (12, True, "test"), ("[10:]", 12, 15), ("[3]", True, 2.5)):
            with pytest.raises(ValueError, match="el argumento"):
                next(single_test_instance.leer_datos_csv(*bad_type))
        # single seek valid parameters
        search = {"[2:]": [list(range(2, 21)), 19], "[1:4]": [list(range(1, 5)), 4], "[11]": [[11], 1],
                  "[0:0]": [[], 0], "[0-5]": [[5], 1], "[2-12-1-7-18]": [[1, 2, 7, 12, 18], 5],
                  "[1-12-14-15-6-7-9-10-11-2]": [[1, 2, 6, 7, 9, 10, 11, 12, 14, 15], 10],
                  "[25]": [[], 0], "[0:5]": [list(range(1, 6)), 5],
                  # and operator
                  '"size" = 16': [[5, 8, 10, 20], 4], '"size" = 9 & "solving_time" = 0.0': [[2, 12], 2],
                  '"size" = 9 & "solving_time = 0.0': [[], 0], '"size" = 9 "solving_time" = 0.0': [[], 0],
                  '"size" = 9 & "solving_time": 0.0': [[], 0], '"size" = 9 % "solving_time" = 0.0': [[], 0],
                  '"size" = 9 & "solving_time" = 0 & "difficulty" = 51': [[12], 1],
                  '"size" = 9 & "solving_time" = 0.0 | "difficulty" = 51': [[2, 12], 2],
                  '"size"=4|"size"=9': [[], 0], '"size" = 4 | "size" = 9 | "size" = 16': [list(range(1, 21)), 20],
                  '"size" = 9 | "solving_time" = 0': [[1, 2, 3, 4, 6, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19], 16],
                  '"size" = 4 & "size" = 9 & "size" = 16': [[], 0],
                  '"size" = 4 & "size" = 9 | "size" = 16': [[5, 8, 10, 20], 4],
                  '"size" < 4': [[], 0], '"size" <= 4': [[1, 3, 4], 3], '"size" != 4': [[2] + list(range(5, 21)), 17],
                  '"size" > 4': [[2] + list(range(5, 21)), 17], '"size" => 4': [[], 0],
                  '"size" >= 16': [[5, 8, 10, 20], 4],
                  '"size" > 0': [list(range(1, 21)), 20], '"size" < 100': [list(range(1, 21)), 20],
                  '"size" > test': [[], 0],
                  '"size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3],
                  '"difficulty" < 6800 & "difficulty" >= 5000 | "size" < 4 & "solving_time" != 4.047': [[20], 1],
                  # test for [= and ]= operators
                  '![SOLVING_TIME#START_VALS] "start" [= 11 | "end" ]= 56 & "size" > 4 & "difficulty" <= 5000': [[14, 16], 2],
                  '"SOLVING_TIME" [= 0.0': [[1, 2, 3, 4, 12, 13, 14, 16], 8],
                  '"SOLVING_TIME" [= 0.09': [[13, 16], 2],
                  # if you chain more than 4 the last ones get combined into one "solving_time" != (4.047 & "size" < 20)
                  '"size" > 4 & "difficulty" >= 6000 & "difficulty" < 7000 & "solving_time" != 4.047 & "size" < 20': [
                      [10, 20], 2],
                  # checking if old system is recognized
                  "size=16": [[], 0], "size=9~solving_time=0.0": [[], 0],
                  "size=9~solving_time=0.0~difficulty=51": [[], 0], "size=9~solving_time=0.0 difficulty=51": [[], 0],
                  "size=9~solving_time=0.0 solving_time=0.312": [[], 0], "size=9 solving_time=0.0": [[], 0],
                  "size=10": [[], 0], "1b5": [[5, 20], 2], "": [list(range(1, 21)), 20], "size: 9": [[], 0]}
        for query, value in search.items():
            collect_entries = []
            for item in single_test_instance.leer_datos_csv(query):
                collect_entries.append(item[0])
            collect_entries.pop(0)
            assert len(collect_entries) == value[1], f"fallo en cantidad encontrada: {query}"
            assert collect_entries == [f"[{val}]" for val in value[0]], f"fallo en buscar entrada: {query}"
        head_1 = ["INDICE", "START", "END", "START_VALS", "SOLVING_TIME", "DIFFICULTY", "SIZE"]
        head_2 = ["INDICE", "START_VALS", "SOLVING_TIME", "DIFFICULTY", "SIZE"]
        head_3 = ["INDICE", "SOLVING_TIME", "DIFFICULTY", "SIZE"]
        head_4 = ["INDICE", "START", "END"]
        head_5 = ["INDICE", "START", "END", "START_VALS"]
        head_6 = ["INDICE", "START", "END", "SIZE"]
        special_query = {
            ('"size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 7, head_1],
            ('![] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 7, head_1],
            ('[] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 7, head_1],
            '[indice] "indice" <= 10': [list(range(1, 11)), 10, 1, ["INDICE",]],
            # space inside [] is not permitted
            ('![size  ] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[], 0, 7, head_1],
             # index col is always kept
            ('![indice] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 7, head_1],
            ('![start_] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 7, head_1],
            '![START#END] "size" > 4': [[2] + list(range(5, 21)), 17, 5, head_2],
            '[START#END] "size" > 4': [[2] + list(range(5, 21)), 17, 3, head_4],
            '![START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [
                [5, 6, 20], 3, 5, head_2],
            '[START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [
                [5, 6, 20], 3, 3, head_4],
            ('![START#END#start_vals] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 4, head_3],
            # syntax error can only chain one function to a query
            ('![START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:1:AVG:SIZE'): [[], 0, 7, head_1],
            ('[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047'): [[5, 6, 20], 3, 4, head_5],
            # those limit test are better formatted for this section
            ('[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:'): [[5, 6, 20], 3, 4, head_5],
            ('[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:START'): [[5, 6, 20], 3, 4, head_5],
            ('[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:0'): [[5, 6, 20], 3, 4, head_5],
            ('![START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:-12'): [[5, 6, 20], 3, 4, head_3],
            ('![START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:12'): [[5, 6, 20], 3, 4, head_3],
            ('[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:2'): [[5, 6], 2, 4, head_5],
            ('[START#END#START_VALS] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~ASC:difficulty'): [[5, 6, 20], 3, 4, head_5],
            ('[START#END#START_VALS] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~ASC:start_vals'): [[6, 20, 5], 3, 4, head_5],
            ('[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~DESC:start_vals'): [[5, 20, 6], 3, 4, head_5],
             '[START#END#SIZE] "START" ]= | | "END" ]= G~DESC:SIZE': [[5, 1], 2, 4, head_6],
            }
        for exclude_query, values in special_query.items():
            collect_entries = []
            new_query = single_test_instance.leer_datos_csv(exclude_query)
            current_head = next(new_query)
            for special_query in new_query:
                collect_entries.append(special_query[0])
            assert len(current_head) == values[2], f"fallo en igualdad de cantidad de columnas: {current_head}"
            assert current_head == values[-1], f"fallo en igualdad de encabezado: {current_head}"
            assert len(collect_entries) == values[1], f"fallo en cantidad encontrada: {exclude_query}"
            assert collect_entries == [f"[{val}]" for val in values[0]], f"fallo en buscar entrada: {exclude_query}"
        # se produce cuando ningún nombre de las columnas especificada coinciden después de la selección de columnas
        syntax_error = single_test_instance.leer_datos_csv(
            '[solving_time#difficulty#size] "dificulty" > 10 & "dificulty" < 100')
        # header
        next(syntax_error)
        assert next(syntax_error) == "error de sintaxis"
        # all of this needs refactoring
        functional_queries = {
            ('![START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" '
             '> 6700 & "solving_time" != 4.047~COUNT:'): [[], 3, 5, head_2],
            ('[START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" '
             '> 6700 & "solving_time" != 4.047~COUNT:difficulty'): [[], 3, 3, head_4],
            ('[START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" '
             '> 6700 & "solving_time" != 4.047~COUNT:dificulty'): [[], 3, 3, head_4],
            ('![START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" '
             '> 6700 & "solving_time" != 4.047~AVG:difficulty'): [[], 80245, 5, head_2],
            ('![START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" > '
             '6700 & "solving_time" != 4.047~MIN:difficulty'): [[], 6715, 5, head_2],
            ('![START#END] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047~MIN:size'): [[], 9, 5, head_2],
            ('![START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" '
             '> 6700 & "solving_time" != 4.047~MAX:difficulty'): [[], 136087, 5, head_2],
            ('[START#END] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047~MAX:difficulty'): [[5, 6], "abg6|", 3, head_4],
            ('[START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" '
             '> 6700 & "solving_time" != 4.047~SUM:start'): [[], 0, 3, head_4],
            ('![START#END] "size" > 4 & "difficulty" >= 5000 | '
             '"difficulty" > 6700 & "solving_time" != 4.047~SUM:size'): [[], 41, 5, head_2],
             # 0.469, 0.141, 0.125, 0.188 = 0.923 to int is 0 is the target sum
             '![INDICE] "solving_time" [= 0.4 | "solving_time" [= 0.1~SUM:solving_time': [[], 0, 7, head_1],
            }

        for functional_queries, numbers in functional_queries.items():
            collect_functional = []
            query_func = single_test_instance.leer_datos_csv(functional_queries)
            func_head = next(query_func)
            for func_result in query_func:
                collect_functional.append(func_result)
            last_item = collect_functional.pop()
            collect_functional = [val[0] for val in collect_functional]
            assert len(func_head) == numbers[2], f"fallo en igualdad de cantidad de columnas: {func_head}"
            assert func_head == numbers[-1], f"fallo en igualdad de encabezado: {func_head}"
            if isinstance(numbers[1], str):
                assert last_item[1][0:5] == numbers[
                    1], f"fallo en resultado para argumento función filtrado en columnas incluidas {numbers[1]}"
            else:
                assert int(last_item[-1]) == numbers[1], f"fallo en resultado función: {functional_queries}"
            assert collect_functional == [f"[{val}]" for val in
                                          numbers[0]], f"fallo en buscar entrada: {functional_queries}"

        # this is an implicit test if this method fails then the next assertions will also fails
        SingleCsvManager.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, new_name="seek_times")
        # test date data on search and operations, accepted format is ISO8601
        # repopulating with new data
        date_instance_test = SingleCsvManager("seek_times", self.DateObjectTest, "#")
        searching_test = {'"DATE" <= 2024-08-20': [[1, 5, 8], 3],
                          '"date" < 06-11-2000': [[], 0],
                          '"DATE" = 2024-08-24': [[7, 11], 2],
                          '"DATE" != 2024-08-21': [list(range(1, 10)) + [11], 10],
                          '"DATE" > 2024-08-21 & "date" <= 2024-08-24': [[7, 9, 11], 3],
                          '"DATE" > 2024-08-21 & "date" <= 2024-08-24~AVG:date': [[0], 1],
                          '"DATE" <= 2024-08-20~MAX:date': [["2024-08-20"], 1],
                          '"DATE" <= 2024-08-20~MIN:date': [["1994-08-26"], 1],
                          '"DATE" <= 2024-08-20~DESC:date': [[1, 8, 5], 3],
                          '"DATE" <= 2024-08-20~ASC:date': [[5, 8, 1], 3],
                          # now unique automatically returns the total amount
                          # of rows (considering the repeated ones) at the end
                          # the last number in the list inside the list
                          # is the total amount of entries for [[1, 5, 2], 3] 
                          # in [1, 5, 2] 2 is the amount of entries (1 and 5 
                          # and the repeated value that is not yielded) 
                          # and 3 is the amount of returned values
                          '"DATE" <= 2024-08-20~UNIQUE:age': [[1, 5, 2], 3],
                          # if the row that you are using a function on
                          # is not requested the function won't apply
                          '[date] "DATE" = 2024-08-24~UNIQUE:age': [[7, 11], 2],
                          '[date] "DATE" = 2024-08-24~UNIQUE:DATE': [[7, 1], 2],
                          '"DATE" < 2024-08-24~MAX:JOB': [["Web Developer"], 1],
                          '"DATE" < 2024-08-24~MIN:JOB': [["HR Coordinator"], 1],
                          '"DATE" < 2024-08-24~AVG:JOB': [[0], 1],
                          '"DATE" < 2024-08-24~SUM:JOB': [[0], 1],
                          '"DATE" < 2024-08-24~UNIQUE:JOB': [[1, 5, 8, 10, 4], 5],
                          '"DATE" < 2024-08-22~ASC:JOB': [[8, 5, 1, 10], 4],
                          '"DATE" < 2024-08-22~DESC:JOB': [[10, 1, 5, 8], 4],
                          # it returns floats like MAX AVG and SUM
                          '"INDICE" > 0~MIN:AGE': [[float(22),], 1],
                          '"NAME" [= j | "AGE" ]= 8 | "AGE" < 24~UNIQUE:JOB': [[1, 2, 4, 7, 8, 10, 6], 7]
                          }
        for query_date, value_date in searching_test.items():
            collect_entries = []
            for dates in date_instance_test.leer_datos_csv(query_date):
                if dates[0] not in ("AVG", "MAX", "MIN", "SUM", "UNIQUE"):
                    collect_entries.append(dates[0])
                else:
                    collect_entries.append(f"[{dates[-1]}]")
            collect_entries.pop(0)
            assert len(collect_entries) == value_date[1], f"fallo en cantidad encontrada: {query_date}"
            assert collect_entries == [f"[{val}]" for val in value_date[0]], f"fallo en buscar entrada: {query_date}"
        # here we make the csv have columns which data that can
        # be a valid float str or dates changing the results
        # of some functions
        date_instance_test.set_data("Sara Stew", "Cape Coral", "age", "Google Intern", "WWWWWWWWW").guardar_datos_csv()
        # making new AVG, SUM, MIX, MAX ASC and DESC test for str
        # to make sure that they are only applied if all the items
        # on a same column are str, since the program tries to 
        # cast into a float or date a str if it can but if the column
        # later on has something that is not a date or a float the program 
        # instead of ignoring those values should treat all the values in the column
        # as a str this means return 0 for SUM and AVG even if some of the data
        # on a column where valid floats or dates and for MIN MAX and DESC if any of
        # the data is not a float or date then all the data should be treated as str
        str_date_test = {
                         # since date is specified for the comparison
                         # it automatically skips entries with invalid
                         # dates that's why those are valid
                         '"DATE" <= 2024-08-20~MAX:DATE': [["2024-08-20"], 1],
                         '"DATE" <= 2024-08-20~MIN:DATE': [["1994-08-26"], 1],
                         '"DATE" < 2024-08-24~AVG:AGE': [[(26 + 33 + 28 + 37 + 28)/5], 1],
                         '"DATE" < 2024-08-24~SUM:AGE': [[float(152)], 1],
                         # here no date is skipped so we get to an invalid
                         # one and thus every dates becomes an str for
                         # the function
                         '"INDICE" > 0~AVG:AGE': [[0], 1],
                         '"INDICE" > 0~SUM:AGE': [[0], 1],
                         '"INDICE" > 0~MIN:AGE': [[22], 1],
                         '"INDICE" > 0~MAX:AGE': [["age"], 1],
                         '"INDICE" > 0~MAX:DATE': [["WWWWWWWWW"], 1],
                         '"INDICE" > 9~ASC:DATE': [[10, 11, 12], 3],
                         '"INDICE" > 9~DESC:DATE': [[12, 11, 10], 3],
                         # to use ]= or [= on index you pass the number not [n or n]
                         '"INDICE" ]= 1~MIN:AGE': [[float(28)], 1],
                         # test for the size operator <>
                         '"name" <> 23': [[], 0],
                         '"age" <> hola': [[], 0],
                         '"city" <> 7': [[3, 4, 5], 3],
                         '"city" <> 6': [[9, 11], 2],
                         '"city" >< 7': [[1, 2, 6, 7, 8, 9, 10, 11, 12], 9],
                         # new string matching operators [] is in and ][ not in
                         '"age" [] r': [[], 0],
                         '"age" [] 4': [[2, 3, 7], 3],
                         '"age" ][ y': [list(range(1,13)), 12],
                         '"age" ][ 28': [[2, 3, 4, 5, 6, 7, 9, 10, 11, 12], 10],
                         }
        for multi_type_col_query, str_val in str_date_test.items():
            get_entries = []
            for data in date_instance_test.leer_datos_csv(multi_type_col_query):
                if data[0] not in ("AVG", "MAX", "MIN", "SUM"):
                    get_entries.append(data[0])
                else:
                    get_entries.append(f"[{data[-1]}]")
            get_entries.pop(0)
            assert len(get_entries) == str_val[1], f"fallo en cantidad encontrada: {multi_type_col_query}"
            assert get_entries == [f"[{val}]" for val in str_val[0]], f"fallo en buscar entrada: {multi_type_col_query}"

    def test_update(self):
        """ comprueba que solo sean actualizadas las filas requeridas por la consulta 
        y solo las columnas especificadas"""
        # setting data back on track
        SingleCsvManager.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, new_name="UPDATE_TIMES")
        test_update_instance = SingleCsvManager("UPDATE_TIMES", delimiter="#")
        with pytest.raises(ValueError, match="debe ingresar un str"):
            # you have to use next in generators for the code to start executing
            next(test_update_instance.actualizar_datos(update_query={1, 4, 6}))
        invalid_update_queries = {
            'UPDATE:~': 'error de sintaxis',
            'UPDATE "JOB"= ON "INDICE" = 2': 'error de sintaxis',
            'UPDATE:~"INDICE"=12 ON "DATE" >= 2024-08-20': 'no se puede actualizar el valor del indice',
            'UPDATE:~"DATE"=1900-06-06 ON ![DATE#NAME] "DATE" <= 2024-08-20': 'la columna a actualizar debe estar dentro de la consulta',
            'UPDATE:~"SALARY"=94.00 ON  ': 'la columna a actualizar debe estar dentro de la consulta',
            'UPDATE:~"CITY"=Santiago ON "DATE" >= 2025-07-04': 'no se encontraron entradas para actualizar',
            'UPDATE:~"NAME"=NONE ON [NAME#DATE#AGE] "AGA" > 10 & "AGA" < 100': 'error de sintaxis',
            'UPDATE:~"DATE"=%MUL:~10 ON "INDICE" = 2': [('solo es posible aplicar una función %ADD o %SUB sobre una fecha '
                                                        'y su elección fue %MUL entrada [2] no actualizada'), "DATE"],
            'UPDATE:~"DATE"=%ADD:~-17 ON "INDICE" < 2': [('superado el número de días que se puede añadir o restar a una fecha '
                                                         '(entre 1 y 1000) ya que su valor fue -17 entrada [1] no actualizada'), "DATE"],
            'UPDATE:~"DATE"=%ADD:~1001 ON "INDICE" = 9': [('superado el número de días que se puede añadir o restar a una fecha '
                                                         '(entre 1 y 1000) ya que su valor fue 1001 entrada [9] no actualizada'), "DATE"],
            # DATE ONLY SUPPORT INT FOR ADD AND SUB
            'UPDATE:~"DATE"=%SUB:~15.09 ON "INDICE" > 7 & "JOB" = HR Coordinator': [('no se puede aplicar una función que no sea %ADD sobre un str '
                                                                                    'y su elección fue %SUB entrada [8] no actualizada'), "DATE"],
            'UPDATE:~"NAME"=%DIV:~11 ON "INDICE" > 7 & "JOB" = HR Coordinator': [('no se puede aplicar una función que no sea %ADD sobre un str '
                                                                                 'y su elección fue %DIV entrada [8] no actualizada'), "NAME"],
            'UPDATE:~"JOB"=%COPY:~INDICE ON "NAME" [= Jan': [("la función %COPY solo se puede usar para copiar el valor de una columna a otra para lo cual "
                                                             f"debe seleccionar el nombre de una columna, su valor fue INDICE pero las opciones son {test_update_instance.new_head[1:]} "
                                                              "entrada [2] no actualizada"), "JOB"],
            'UPDATE:~"CITY"=%COPY:~SALARY ON "NAME" ]= NEZ': [("la función %COPY solo se puede usar para copiar el valor de una columna a otra para lo cual "
                                                             f"debe seleccionar el nombre de una columna, su valor fue SALARY pero las opciones son {test_update_instance.new_head[1:]} "
                                                              "entrada [8] no actualizada"), "CITY"],
            'UPDATE:~"AGE"=%DIV:~USE:~CITY ON "INDICE" = 5': [('no se puede aplicar una función que no sea %ADD sobre un str '
                                                               'y su elección fue %DIV entrada [5] no actualizada'), "AGE"],
            'UPDATE:~"AGE"=%RANDOM-INT:~5.67#CASA ON "INDICE" = 11': [("para usar la función %RANDOM-INT debe pasar dos números enteros como limites inferior "
                                                                      f"y superior pero introdujo 5.67 y CASA entrada [11] no actualizada"), "AGE"],
            'UPDATE:~"DATE"=%NUM-FORMAT:~3 ON "INDICE" = 4': [("para usar la función %NUM-FORMAT debe ocupar una columna que contenga valores decimales o enteros "
                                                               "pero el valor fue de tipo str"), "DATE"],
            # int("10.01") => ValueError int(10.01) => 10
            'UPDATE:~"AGE"=%NUM-FORMAT:~10.01 ON "INDICE" = 7': [("para usar la función %NUM-FORMAT debe pasar como argumento un número entero "
                                                  "pero el valor fue de tipo str"),"AGE"],
            'UPDATE:~"AGE"=%NUM-FORMAT:~0 ON "INDICE" = 10': [("el segundo argumento para la función %NUM-FORMAT "
                                             "debe ser un número entre 1 y 25 pero fue 0"), "AGE"],
            'UPDATE:~"AGE"=%NUM-FORMAT:~-13 ON "INDICE" = 3': [("el segundo argumento para la función %NUM-FORMAT "
                                                "debe ser un número entre 1 y 25 pero fue -13"), "AGE"],
            'UPDATE:~"AGE"=%NUM-FORMAT:~26 ON "INDICE" = 2': [("el segundo argumento para la función %NUM-FORMAT "
                                              "debe ser un número entre 1 y 25 pero fue 26"), "AGE"],
        }
        for bad_request, message in invalid_update_queries.items():
            update_attempt = next(test_update_instance.actualizar_datos(update_query=bad_request))
            if isinstance(update_attempt, str):
                assert message in update_attempt, f"fallo en query {bad_request}"
            else:
                assert message[0] in update_attempt["errors"][message[1]], f"fallo en query {bad_request}"
                assert not update_attempt["old"][message[1]], f"fallo en query {bad_request}"

        valid_update_queries = {
            # [col] might matter on how the results are returned
            'UPDATE:~"DATE"=1900-06-06 ON "DATE" <= 2024-08-20': [[1, 5, 8], {5: "1900-06-06"}],
            # anything that has the string son on it
            'UPDATE:~"DATE"=2025-01-01 ON son': [[3, 5, 9], {5: "2025-01-01"}],
            'UPDATE:~"NAME"=Lucas Folch "AGE"=30 ON [NAME#AGE] "INDICE" >= 2 & "INDICE" < 8': [list(range(2,8)), {1: "Lucas Folch", 3: "30"}],
            'UPDATE:~"NAME"=Joe Biden "JOB"=Old Man ON ![AGE] "JOB" = Project Manager~MIN:DATE': [[5, 9], {1: "Joe Biden", 4: "Old Man"}],
            'UPDATE:~"NAME"=Musk "JOB"=Conman ON "AGE" > 30~MAX:JOB': [[9], {1: "Musk", 4: "Conman"}],
            'UPDATE:~"AGE"=70 ON [AGE#NAME] "NAME" = Musk~AVG:JOB': [[9], {3: "70"}],
            'UPDATE:~"JOB"=Django Developer ON "NAME" = Lucas Folch~UNIQUE:NAME': [[2, 3, 4, 6, 7], {4: "Django Developer"}],
            'UPDATE:~"CITY"=Nuku\'alofa "JOB"=Tourist ON ![DATE] "INDICE" > 9 | "CITY" = New York~ASC:': [[1, 10, 11], {2: "Nuku\'alofa", 4: "Tourist"}],
            'UPDATE:~"NAME"=Wes "Name"=Luke ON "AGE" >= 70~DESC:': [[9], {1: "Luke"}],
            'UPDATE:~"NAME"=Xenon ON [NAME#JOB] "INDICE" = 1~COUNT:': [[1], {1: "Xenon"}],
            'UPDATE:~"NAME"=Charles "JOB"=Ex-Convict ON ![INDICE] "name" = Xenon~SUM:AGE': [[1], {1: "Charles", 4: "Ex-Convict"}],
            'UPDATE:~"CITY"=Ankara "CITY"=Ankara ON "CITY" = Philadelphia~LIMIT:2': [[6], {2: "Ankara"}],
            'UPDATE:~"DATE"=1594-08-15 "NAME"=Rem "JOB"=Oni ON "DATE" <= 2024-08-20': [[1, 8], {1: "Rem", 4: "Oni", 5: "1594-08-15"}],
            ('UPDATE:~"NAME"=Lilith "CITY"=Tokyo "AGE"=100000 "JOB"=Goddess ' 
             '"DATE"=2010-07-11 ON "CITY" = Ankara'): [[6], {1: "Lilith", 2: "Tokyo", 3: "100000", 4: 'Goddess "DATE"=2010-07-11'}],
            'UPDATE:~"NAME"=%UPPER "AGE"=%LOWER "CITY"=%LOWER ON "INDICE" = 8~SUM:AGE': [[8], {1: "REM", 2: "san diego", 3: "28"}],
            'UPDATE:~"NAME"=%CAPITALIZE "CITY"=%TITLE ON "INDICE" = 8': [[8], {1: "Rem", 2: "San Diego"}],
            'UPDATE:~"NAME"=%REPLACE:~a#a ON "INDICE" = 8': [[8], {1: "Rem"}],
            'UPDATE:~"NAME"=%REPLACE:~e#a "AGE"=%ADD:~100.2 "AGE"=%FLOOR "AGE"=%REPLACE:~8#R ON "INDICE" = 8': [[8], {1: "Ram", 3: "12R"}],
            # SINCE THE OPERATIONS ARE DONE AS A FLOAT THE RESULTS ARE RETURNED WITH A .0 IF THEY WERE INT BEFORE
            'UPDATE:~"AGE"=%ADD:~45 "AGE"=%REPLACE:~R#1 "AGE"=%MUL:~17 ON "INDICE" = 8': [[8], {3: "206465.0"}],
            'UPDATE:~"DATE"=%ADD:~971 "DATE"=%SUB:~15 "DATE"=%ADD:~#1998-11-27 ON "DATE" <= 1594-08-15': [[1, 8], {5: "1597-03-28#1998-11-27"}],
            'UPDATE:~"AGE"=100000 "AGE"=%DIV:~0 "AGE"=%ADD:~0.4 "AGE"=%CEIL ON "name" = Lilith': [[6], {3: "100001"}],
            # TEST FLOAT ON INF AND NAN
            'UPDATE:~"AGE"=%MUL:~-inf "AGE"=%ADD:~inf ON "CITY" [= san': [[7, 8], {3: "nan"}],
            'UPDATE:~"AGE"=inf "AGE"=%MUL:~-inf ON "INDICE" = 8': [[8], {3: "-inf"}],
            'UPDATE:~"AGE"=%UPPER "AGE"=%ADD:~inf ON "INDICE" [= 8': [[8], {3: "nan"}],
            'UPDATE:~"NAME"=MARK ON "INDICE" ]= 1': [[1, 11], {1: "MARK"}],
        }
        for valid_request, results in valid_update_queries.items():
            values_updated = []
            for new_result in test_update_instance.actualizar_datos(update_query=valid_request):
                values_updated.append(new_result["result"][0])
                for key, val in results[1].items():
                    assert new_result["result"][key] == val, f"fallo valor en query {valid_request} {new_result['result']}"
            assert values_updated == [f"[{val}]" for val in results[0]], f"fallo entrada en query {valid_request}"

        # structure first item no updated rows, second number of errors and third the updated values
        parcial_valid_queries = {'UPDATE:~"AGE"=%ADD:~BAD ON [1-3-5-7-9-11]': [[], [], {3: [f"{x}BAD" for x in [28, 45, 37, 40, 33, 29]]}],
                                 'UPDATE:~"AGE"=%DIV:~2 ON [1-3-5-7-9-11]': [[1, 3, 5, 7, 9, 11], [6], {3: []}],
                                 # all arithmetic operations return a float use ceil or floor to get back to int
                                 'UPDATE:~"AGE"=%MUL:~1 ON "INDICE" > 0': [[1, 3, 5, 7, 9, 11], [6], {3: [str(float(x)) for x in [34, 22, 31, 28, 26]]}],
                                 'UPDATE:~"AGE"=%DIV:~3 "AGE"=%SUB:~4 "AGE"=%CEIL ON "INDICE" > 0': [[1, 3, 5, 7, 9, 11], [18], {3: [str(x) for x in [8, 4, 7, 6, 5]]}],
                                 'UPDATE:~"AGE"=%MUL:~2.1 "AGE"=%FLOOR ON [2-4-6-8-10]': [[], [], {3: [str(x) for x in [16, 8, 14, 12, 10]]}],
                                 'UPDATE:~"AGE"=%MUL:~-1 "DATE"=%ADD:~10500 ON [2-4-6-8-10-11]': [[11], [7], {3: [str(float(x)) for x in [-16, -8, -14, -12, -10]]}],
                                 }
        # index can be use as a away to reset the current data
        SingleCsvManager.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, new_name="UPDATE_TIMES_2")
        update_test_two = SingleCsvManager("UPDATE_TIMES_2", delimiter="#")
        for parcial_query, parcial_result in parcial_valid_queries.items():
            total_invalids = parcial_result[1][0] if parcial_result[1] else 0
            for query_result in update_test_two.actualizar_datos(parcial_query):
                if isinstance(query_result, dict):
                    total_invalids -= sum([len(val) for val in query_result["errors"].values()])
                    if query_result["result"][0] in [f"[{item}]" for item in parcial_result[0]]:
                        # 0 is index, 1 is message if none where updated
                        assert query_result["result"][1] == "ningún valor de la fila fue actualizado, todas la operaciones fueron invalidas", f"fallo valor en query {parcial_query} {query_result['result']}"
                    else: 
                        for key, val in parcial_result[2].items():
                            assert query_result["result"][key] in val, f"fallo valor en query {parcial_query} {query_result}"           
                else:
                    raise AssertionError(f"resultado de tipo inesperado para query {parcial_query} ya que el resultado fue de tipo {type(query_result).__name__} {query_result}")
            assert total_invalids == 0, (f"fallo en cantidad de errores se esperaban {parcial_result[1][0] if parcial_result[1] else 0} "
                                         f"pero hubo {parcial_result[1][0] - total_invalids if parcial_result[1] else 0} en {parcial_query}")
        
        # UPDATE TO "" AND SEARCH WHEN THE VALUE IS EMPTY
        # THE UPDATE NEW VALUE CANNOT BE EMPTY IS NOT RECOGNIZED BY THE REGEX
        # CHECK DIFFERENCE BETWEEN USING BACKUP TRUE AND FALSE
        # FOR INTEGRITY ALWAYS TRY TO USE THE BACKUP TRUE
        # UPDATE:~"AGE"=%SUB:~<other-col-name>

        # REALLY IMPORTANT TO MAKE THE UPDATE VALID YOU NEED TO CONSUME THE 
        # UPDATE ITERATOR COMPLETELY FIRST
        for _ in update_test_two.actualizar_datos('UPDATE:~"JOB"=  ON "INDICE" = 2 | "INDICE" = 11'):
            pass
        rows = update_test_two.leer_datos_csv('"JOB" =  ')
        next(rows)
        assert next(rows)[0] == "[2]", "fallo en actualizar y buscar 2"
        assert next(rows)[0] == "[11]", "fallo en actualizar y buscar 11"

    def test_create_writer(self):
        """ comprueba el funcionamiento del método estático create_writer"""
        for arguments in  (["invalid name", "valid_name"], ["yield", "no_keyword_name"], ["class", "456"]):
            with pytest.raises(ValueError, match="el encabezado del archivo contiene caracteres inválidos"):
                SingleCsvManager.create_writer(*arguments)

    def test_index_write(self):
        """ comprueba que se cree una clase nueva cuando se pasa un csv que no depende de
        objetos de python y que la clase creada permita agregar valores"""
        write_object = SingleCsvManager.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, new_name="INDEX_WRITER")
        test_instance = SingleCsvManager(file_name="INDEX_WRITER", current_class=write_object, delimiter="#")
        test_instance.set_data(**{"name": "Finn", "city": "Port Vila", 
                                   "age": "25", "job": "Developer", "date": "1999-08-17"})
        new_entry = test_instance.guardar_datos_csv()
        assert new_entry == "\nINDICE#NAME#CITY#AGE#JOB#DATE\n[12]#Finn#Port Vila#25#Developer#1999-08-17", f"error al crear entrada {new_entry}"
        updated_entry = test_instance.actualizar_datos(update_query='UPDATE:~"NAME"=Jake "AGE"=35 ON "INDICE" = 12')
        expected = next(updated_entry)["result"] 
        assert expected == "[12]#Jake#Port Vila#35#Developer#1999-08-17".split("#"), f"error al actualizar entrada {'#'.join(expected)}"


    def test_index_exclude_include(self):
        """ comprueba el funcionamiento de los argumentos id_present, extra_columns y new_name
        del método de clase index"""
        invalid_combinations = [(set(), []), ((1,), "HOLA"), 
                                ({}, 12.6), ([], []), ({}, {})]
        for extra, excluded in invalid_combinations:
            with pytest.raises(ValueError, match="pero su argumento fue de tipo"):
                SingleCsvManager.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, extra_columns=extra, exclude=excluded)

        # the program first exclude the cols and then add the new ones
        # so this ({"UNIVERSITY": "MIT", "JOB": " "}, ["JOB", "NAME"]) does not
        # trow an error
        invalid_new_col_names = [({"SALARY": "", "NAME": "NULL"}, None),
                                 ({"SALARY": "", "salary": "NULL"}, ["name"])]
        for new_col, removed_col in invalid_new_col_names:
            with pytest.raises(ValueError, match="los encabezados no pueden tener nombres repetidos para las columnas"):
                SingleCsvManager.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, extra_columns=new_col, exclude=removed_col)

        # resetting the case time data
        all_exclude_col_object = SingleCsvManager.index(file_path=str(self.case_time_data), 
                                              delimiter="#", id_present=False, exclude=["INDICE", "NAME", "Job", "DATE", "city", "aGe"], new_name="INDEX_EXCLUDE")
        test_instance_2 = SingleCsvManager(file_name="INDEX_EXCLUDE", current_class=all_exclude_col_object, delimiter="#")

        result_instance_2 = test_instance_2.leer_datos_csv()
        assert next(result_instance_2) == ["INDICE"], "fallo en encabezado"
        assert len(next(result_instance_2)) == 1, "fallo en cantidad esperada"
        # testing what happens when writing class with empty __dict__
        # it should write the new index with no content
        test_instance_2.set_data().guardar_datos_csv()
        for search_result in test_instance_2.leer_datos_csv('"INDICE" > 0'):
            pass
        assert search_result == ["[12]",]


        added_col_object = SingleCsvManager.index(file_path=str(self.case_time_data), new_name="INDEX_EXTRA",
                                              delimiter="#", id_present=False, exclude=["city",], extra_columns={"SALARY": "", 
                                                                                                                 "TAX": "25",
                                                                                                                 "TOTAL": "0", 
                                                                                                                 "CITY": " "})
        test_instance_3 = SingleCsvManager(file_name="INDEX_EXTRA", current_class=added_col_object, delimiter="#")
        for value in test_instance_3.leer_datos_csv(search='[SALARY#CITY] "INDICE" > 0~UNIQUE:SALARY'):
            # for this case with unique only one result is expected
            if value[0] not in ("UNIQUE", "INDICE"):
                assert value[0] in [f"[{i}]" for i in range(1, 2)], f"fallo en cantidad encontrada {value}"
                assert all([val == "VOID" for val in value[1:]])
        test_instance_3.set_data(**{"name": "Lian", "age": 31, "job": "DevOps",
                                     "date": "2016-07-24", "salary": 2500, "tax": 15,
                                     "total": 2500 - 2500 * 0.15, "city": "Ohio",})       
        assert test_instance_3.guardar_datos_csv() == ("\nINDICE#NAME#AGE#JOB#DATE#SALARY#TAX#TOTAL#CITY\n[12]"
                                                       "#Lian#31#DevOps#2016-07-24#2500#15#2125.0#Ohio")
        queries = [
            # cant pass float to random-int
            'UPDATE:~"SALARY"=%RANDOM-INT:~4560#900 "TOTAL"=%COPY:~SALARY "TOTAL"=%MUL:~USE:~TAX "TOTAL"=%DIV:~100 ON "INDICE" < 11',
            'UPDATE:~"TOTAL"=%MUL:~-1 "TOTAL"=%ADD:~USE:~SALARY "TOTAL"=%FLOOR ON "INDICE" < 11',
        ]
        for request in queries:
            for _ in test_instance_3.actualizar_datos(update_query=request):
                pass

        result_instance_3 = test_instance_3.leer_datos_csv('"indice" < 11')
        next(result_instance_3)
        for value in result_instance_3:
            assert value[-1] == "VOID"
            assert int(value[5]) <= 4560
            assert int(value[5]) >= 900
            assert int(value[7]) <= 3420
            assert int(value[7]) >= 675

        # TEST FOR NEW INDEX FEATURES WITH A CSV ALREADY INDEXED
        write_object_with_index = SingleCsvManager.index(file_path=str(self.case_data_single), delimiter="#", new_name="INDEXED-CSV", 
                                                         extra_columns={"time_difficulty_ratio": ""}, exclude=["start", "start", "end",])
        test_instance_indexed = SingleCsvManager(file_name="INDEXED-CSV", current_class=write_object_with_index, delimiter="#")
        test_instance_indexed.set_data(**{"start_vals": 90, "solving_time": 603.60, "difficulty": 736425, "size": 16, "time_difficulty_ratio": 0.000820})
        test_result = [0.16, 1.216, 0.286, 0.308, 0.001, 0.0, 0.011, 0.047, 0.03, 0.015, 0.035, 0.608, 0.176, 
                       0.267, 0.099, 0.261, 0.087, 0.067, 0.029, 0.014]
        update_query = ('UPDATE:~"time_difficulty_ratio"=%COPY:~start_vals "time_difficulty_ratio"=%DIV:~USE:~difficulty '
                       '"time_difficulty_ratio"=%NUM-FORMAT:~3 ON "INDICE" > 0')
        for next_test, next_result in zip(test_instance_indexed.actualizar_datos(update_query=update_query), test_result, strict=True):
            assert next_test["result"][-1] == f"{next_result:.3f}"

        # test for col with indice column other than the first
        # it should raise error for both cases
        with pytest.raises(ValueError, match="los encabezados no pueden tener"):
            SingleCsvManager.index(file_path=str(self.case_misplaced_index_col), delimiter=",",
                                   id_present=False, extra_columns={"ACTOR": ""}, exclude=["release year"])
        # TEST ID PRESENT ON FILE WITHOUT ID
        # THIS SHOULD OVERRIDE THE FIRST COL WITH INDICE
        # FOR THIS FILE THE COL NAME IS NO LONGER THERE
        new_object_writer = SingleCsvManager.index(str(self.case_time_data), delimiter="#", new_name="index-rewrite")
        new_instance_case = SingleCsvManager("index-rewrite", new_object_writer, delimiter="#")
        search_iter = new_instance_case.leer_datos_csv('"INDICE" = 1')
        assert next(search_iter) == ["INDICE", "CITY", "AGE", "JOB", "DATE"]
        assert next(search_iter) == ["[1]", "New York", "28", "Software Engineer", "2024-08-20"]
        new_instance_case.set_data(**{"city": "", "age": "", "job": "", "date": ""}).guardar_datos_csv()
        # setting a len selector to check for empty entries
        # <> is the size operator
        # TEST FOR NUMBER OF COL SIZE LIMIT
        for data in new_instance_case.leer_datos_csv('"CITY" <> 0'):
            pass
        assert data == ["[12]", "", "", "", ""]
        # TEST WITH NO INDEX BUT EXCLUDE FIRST VALUE
        for updated in new_instance_case.actualizar_datos('UPDATE:~"CITY"=Ohio "AGE"=21 "JOB"="Manager Assistant" "DATE"=2013-07-22 ON "AGE" <> 0 & "JOB" <> 0'):
            pass
        # you can use "" on values to update and this is how that looks
        assert updated["result"] == ['[12]', 'Ohio', '21', '"Manager Assistant"', '2013-07-22']

        # TEST FOR INDEXED CSV WITH NAME != INDICE USING ID_PRESENT FALSE
        # expected result: you end up we two columns that represent the entry index
        writer = SingleCsvManager.index(str(self.case_different_index_name), ",", False, new_name="DIFFERENT-INDEX")
        new_instance_case = SingleCsvManager("DIFFERENT-INDEX", writer, ",")
        for entry in new_instance_case.leer_datos_csv('"ENTRY_NUMBER" = 5'):
            pass
        assert entry == ["[5]", "5", "2007", "Iron Vanguard", "156600376"]
        new_instance_case.set_data(*[78, 2026, "The Amazing World Of Gumball: The Movie", 250000000])
        write_result = new_instance_case.guardar_datos_csv()
        assert write_result == ("\nINDICE,ENTRY_NUMBER,RELEASE_YEAR,MOVIE_TITLE,GROSS_INCOME\n"
                                "[22],78,2026,The Amazing World Of Gumball: The Movie,250000000")


    def test_delete(self):
        """ comprueba que se borren las entradas especificadas en las queries"""
        # clean up the data always before an assert
        # putting data back on track
        # in case a previous test fails
        SingleCsvManager.index(str(self.case_data_single), delimiter="#", new_name="DELETE_TEST")
        test_instance = SingleCsvManager("DELETE_TEST", delimiter="#")
        invalid_delete = ["hola", "size=9", "[:12]", "[1-12-14-15-6-7-9-10-11-2-14]", "[10-]",
                          "<class 'vehiculo.Bicicleta'>",
                          'DELETE ON[solving_time#difficulty#size] "difficulty" > 10 & "difficulty" < 100',]
        for query in invalid_delete:
            with pytest.raises(ValueError, match="o escribiendo una consulta usando la palabra clave DELETE"):
                next(test_instance.borrar_datos(query))

        # it used to take two args thats why it is still a tuple
        for bad_arg in ((12,), (True,), ([2, 5],)):
            with pytest.raises(ValueError, match="el argumento"):
                next(test_instance.borrar_datos(*bad_arg))

        # borrar dato con indice inexistente
        with pytest.raises(ValueError, match="ninguno de los valores"):
            next(test_instance.borrar_datos("[25]"))

        valid_delete = {"[1]": [[1], 19], "[2:4]": [list(range(2, 5)), 16],
                        "[1-5-10]": [[1, 5, 10], 13], "[3:]": [list(range(3, 14)), 2]}
        for deleted, value in valid_delete.items():
            collect_deleted = []
            for item in test_instance.borrar_datos(deleted):
                collect_deleted.append(str(item).split("#")[0])
            collect_deleted.pop(0)
            assert test_instance.current_rows - 1 == value[1], f"fallo en cantidad borrada: {deleted}"
            assert collect_deleted == [f"[{val}]" for val in value[0]], f"fallo en borrar entrada: {deleted}"

        assert next(test_instance.borrar_datos('DELETE ON "INDICE" = 100')) == "no se encontraron entradas para eliminar"
        assert next(test_instance.borrar_datos('DELETE ON "SIZE" < 4')) == "no se encontraron entradas para eliminar"
        assert next(test_instance.borrar_datos('DELETE ON [solving_time#difficulty#size] "dificulty" > 10 & "dificulty" < 100')) == "error de sintaxis"
        assert next(test_instance.borrar_datos("borrar todo")) == "todo"
        assert next(test_instance.borrar_datos("borrar todo")) == "nada"
        assert next(test_instance.borrar_datos("[1]")) == "nada"
        # overriding old instance to sync original to backup
        SingleCsvManager.index(str(self.case_data_single), delimiter="#", new_name="DELETE_TEST_2")
        test_instance = SingleCsvManager("DELETE_TEST_2", delimiter="#")
        valid_query_delete = {
            ('DELETE ON [START#END#START_VALS] "size" > 4 & "difficulty" >= 5000 '
             '| "difficulty" > 6700 & "solving_time" != 4.047~LIMIT:1'): [[5, 6, 20], 17], 
             'DELETE ON "INDICE" >= 15': [[15, 16, 17], 14],
             # functions should be ignored when using queries
             'DELETE ON "SIZE" < 5~SUM:difficulty': [[1, 3, 4], 11],
             # DELETE ON  is the equivalent to pass " "
             'DELETE ON 99|': [[4, 6, 11], 8],
             'DELETE ON "SOLVING_TIME" [= 0.0 | "START" ]= f': [[1, 3, 5, 6, 7,], 3],
             }
        for del_query, del_entry in valid_query_delete.items():
            collect_deleted = []
            for item in test_instance.borrar_datos(del_query):
                collect_deleted.append(str(item).split("#")[0])
            collect_deleted.pop(0)
            assert collect_deleted == [f"[{val}]" for val in del_entry[0]], f"fallo en borrar entrada: {del_query}"
            assert test_instance.current_rows - 1 == del_entry[1], f"fallo en cantidad borrada: {del_query}"
        

    def test_pass_object_no_dict(self):
        """ chequea que se de una advertencia si se intenta guardar un objeto que no
        soporte __dict__ ya que es lo que se usa para guardar los atributos"""
        # data restoration before the next test is run
        SingleCsvManager.index(str(self.case_data_single), "#")
        assert "Actualmente esta ocupando un objeto de tipo" in SingleCsvManager("no_dict", delimiter="#").guardar_datos_csv()

    def test_max_row_zero(self):
        """ chequea que si se excede la capacidad designada para la cantidad de filas
        se retorne una advertencia al usuario al tratar de crear una nueva entrada"""

        # test class so the object has __dict__
        class RowsTest:
            filler = 0

        test_instance = SingleCsvManager("no_dict", current_class=RowsTest, delimiter="#")
        test_instance.set_data()
        BaseCsvManager.max_row_limit = 0
        assert "Advertencia: Su entrada no fue creada" in test_instance.guardar_datos_csv()
        BaseCsvManager.max_row_limit = 13
        # plus one to not count header
        test_instance.current_rows = 14
        assert "Advertencia: Su entrada no fue creada" in test_instance.guardar_datos_csv()
        # the change has to be reverted since this alters the value for all the classes
        # that inherit from BaseCsvManager
        BaseCsvManager.max_row_limit = 50_000

    def test_max_col_limit(self):
        """comprueba que si se excede el número máximo de
        columnas por archivo se retorne un mensaje de advertencia"""
        BaseCsvManager.max_col_limit = 2
        test_instance = SingleCsvManager("no_dict", self.SingleObjectTest, delimiter="#", exclude=("other",))
        test_instance.set_data(**self.arguments)
        assert "__dict__ que supera el máximo de columnas" in test_instance.guardar_datos_csv()
        BaseCsvManager.max_col_limit = 20

    def test_no_writer_instance(self):
        """ verifica que si no se han pasado datos a writer_instance no se pueda ocupar el método para 
        para guardar datos"""
        new_instance = SingleCsvManager("no_dict", self.SingleObjectTest, delimiter="#")
        assert "usando el método set_data" in new_instance.guardar_datos_csv()

    def test_not_matched_dict_object(self):
        """ comprueba que no se pueden escribir nuevas entrada que no tengan los mismo nombres
        de atributos y cantidad de atributos que las entradas ya presentes aquí, se chequea que
        si al pasar un objeto que no calza con los ya presentes se lance un error"""
        SingleCsvManager.index(str(self.case_data_single), "#", new_name="no_dict")
        new_instance = SingleCsvManager("no_dict", self.SingleObjectTest, delimiter="#")
        new_instance.set_data(**self.arguments)
        # length inequality
        with pytest.raises(ValueError, match="con el mismo número"):
            # if you use this program you shouldn't change from
            # single True to False if this module is already being used for
            # on a mode it will only create bugs
            new_instance.guardar_datos_csv()

        # name inequality
        new_instance.exclude = ("size",)
        with pytest.raises(ValueError, match="atributos y nombres"):
            # if you use this program you shouldn't change from
            # single True to False if this module is already being used for
            # on a mode it will only create bugs
            new_instance.guardar_datos_csv()

    def test_exclude(self):
        """ comprueba que los atributos del objeto excluidos usando el
        atributo exclude sean excluidos al escribir una nueva entrada ('!' al
        inicio de exclude es para negar y que solo se incluya lo después del '!')"""
        local_instance = SingleCsvManager("no_dict", self.SingleObjectTest, "#",
                                          exclude=("other",))
        local_instance.set_data(**self.arguments)
        assert local_instance.guardar_datos_csv() == ("\nINDICE#START#END#START_VALS#SOLVING_TIME#DIFFICULTY#"
                                                      "SIZE\n[21]#-432-3---1-442-3#1432234131244213#9#0.0#8#4")
        local_instance.set_data(**{"start": "-432-3---1-442-3", "end": ":1432234131244213:",
                                    "start_vals": 9, "solving_time": 0.0, "difficulty": 8,
                                    "size": 4, "other": "nada"})
        # comprobando que en modo single = True los : no son remplazados
        assert local_instance.guardar_datos_csv() == ("\nINDICE#START#END#START_VALS#SOLVING_TIME"
                                                      "#DIFFICULTY#SIZE\n[22]#-432-3---1-442-3#:"
                                                      "1432234131244213:#9#0.0#8#4")
        local_instance.exclude = ("!", "other")
        # esto lanza error (como debería) por que es modo single y la cantidad de atributos
        # no calza al negar exclude con los de las entradas ya escritas
        with pytest.raises(ValueError, match="atributos y nombres"):
            local_instance.guardar_datos_csv()

    def test_enforce(self):
        """ comprueba que se pasen los argumentos apropiados al método guardar_datos_csv
        y que al pasar los correctos se aplique la función de enforce_unique que es
        comprobar que solo se puedan escribir nuevas entradas con valores únicos para los
        campos pasados al argumento enforce_unique"""
        local_instance = SingleCsvManager("no_dict", self.SingleObjectTest, "#", exclude=("other",))
        local_instance.set_data(**self.arguments)
        for cases, message in {12: "debe ser una tuple", (): "al menos un str",
                               (True,): "solo debe contener str", ("size", 12): "solo debe contener str"}.items():
            with pytest.raises(ValueError, match=message):
                local_instance.guardar_datos_csv(enforce_unique=cases)
        assert local_instance.guardar_datos_csv(("size",)) == "presente"
        assert local_instance.guardar_datos_csv(("size", "solving_time")) == "presente"

    def test_list_records(self):
        files_instance = [file_name for file_name in BaseCsvManager.return_current_file_names()]
        actual_files = [file.stem for file in BaseCsvManager.backup.iterdir()]
        assert  len(files_instance) == len(actual_files), "fallo en cantidad de archivos"
        assert files_instance == actual_files, "fallo en nombres de archivos"

    def test_rename_folder(self):
        new_instance = SingleCsvManager("times", self.DateObjectTest, "#")
        new_instance.set_data(*["Daniel Joseph", "Mississippi", 65, "Software Engineer", "2004-11-10"]).guardar_datos_csv()
        for entry in new_instance.leer_datos_csv('"indice" > 0~COUNT:'):
            pass
        assert entry[-1] == 12
        with pytest.raises(ValueError, match="debe solo contener los siguientes caracteres"):
            new_instance.rename_file("rename folder")
        new_instance.rename_file("rename_folder")
        current_names_space = [name for name in BaseCsvManager.return_current_file_names()]
        assert "rename_folder" in current_names_space
        assert "times" not in current_names_space
        assert new_instance.instance_file_path.stem == "rename_folder"
        for _ in new_instance.actualizar_datos('UPDATE:~"AGE"=%SUB:~15 "AGE"=%FLOOR ON "INDICE" = 12'):
            pass
        for entry in new_instance.leer_datos_csv('[AGE] "indice" = 12'):
            pass
        assert entry[-1] == "50"

    def test_delete_records(self):
        """ comprueba que este método estático elimine los archivos correspondientes"""
        # invalid name should not
        # be able po pass to path here 
        # since it checks if the name is in the directory first
        current_files = [file_name for file_name in BaseCsvManager.return_current_file_names()]
        BaseCsvManager.delete_record("not_present")
        assert len(current_files) == sum(1 for _ in BaseCsvManager.return_current_file_names())
        # if is not a string is not going to be considered and it should still 
        # delete if any string name matches
        to_delete = [True, "times", "data_single", "DIFFERENT-INDEX", "INDEX_EXTRA", "index-rewrite", 678_900]
        BaseCsvManager.delete_record(*to_delete)
        assert set([file_name for file_name in BaseCsvManager.return_current_file_names()]) & set(to_delete) == set()
        BaseCsvManager.delete_record("INDEX_WRITER", "INDEX_EXCLUDE", "borrar todo")
        assert sum(1 for _ in BaseCsvManager.return_current_file_names()) == 0
        
    def test_multiple_operations(self):
        """ chequea el comportamiento de distintas funciones vistas previamente en una sola ejecución como crear
        un nuevo directorio de respaldos y la escritura entre distintas instancias de SingleCsvManager"""
        # first create a new backup in other folder
        new_dir = Path(fr"{Path(__file__).parent}\last_test_backup")
        if new_dir.is_dir():
            shutil.rmtree(str(new_dir))
        new_dir.mkdir(exist_ok=True)
        BaseCsvManager.backup = new_dir
        # pass only a file name to the init function
        new_instance = SingleCsvManager(file_name="new_file", current_class=self.MovieObjectTest, 
                                        delimiter=",", exclude=("entry",))
        # latter in replace all the headers with the ones in movies.csv
        new_instance.set_data(**{"entry": 1, "release": 2026, "title": "The Amazing World Of Gumball: The Movie",
                                 "income": 250_000_000})
        result = new_instance.guardar_datos_csv()
        assert result == ("\nINDICE,RELEASE,TITLE,INCOME\n"
                          "[1],2026,The Amazing World Of Gumball: The Movie,250000000")
        assert new_instance.guardar_datos_csv(enforce_unique=("title",)) == "presente"
        new_instance.set_data(*['7', '2019', 'Neon Mirage', '254848284']).guardar_datos_csv()
        writer_object = SingleCsvManager.index(file_path=f"{self.case_different_index_name}", delimiter=",", id_present=False)
        another_instance = SingleCsvManager(file_name="movies", current_class=writer_object, delimiter=",")
        for count, entry in enumerate(another_instance.leer_datos_csv('"INDICE" > 0 & "indice" <= 9')):
            # to no count the header
            if count:
            # so we do not count the indice
                new_instance.set_data(*entry[1:]).guardar_datos_csv(enforce_unique=("title",))
        # assert what values where entered
        search_results = [value for value in new_instance.leer_datos_csv('"title" = Phantom\'s Echo')]
        assert len(search_results[1:]) == 1
        # this counts also the header
        current_rows = another_instance.current_rows
        for _ in another_instance.borrar_datos('DELETE ON "INDICE" > 0 & "INDICE" <= 9'):
            pass
        assert another_instance.current_rows == current_rows - 9
        # finally delete the new directory
        # >> is the len grater than operator and << is the len less than operator
        # <> is the len equals to operator there is not inclusive version of << or >>
        updated_values = [value["result"][0] for value in another_instance.actualizar_datos('UPDATE:~"entry_number"=%RANDOM-INT:~502#701 "GROSS_income"=%DIV:~2 "GROSS_income"=%FLOOR ON "MOVIE_TITLE" >> 13 & "movie_title" << 19')]
        assert updated_values == [f"[{val}]" for val in (4, 6, 7, 8, 10, 11)]
        for count, entry in enumerate(another_instance.leer_datos_csv('"MOVIE_TITLE" >> 13 & "movie_title" << 19')):
            if count:
                new_instance.set_data(*entry[1:]).guardar_datos_csv()
        for search_count in new_instance.leer_datos_csv('"INDICE" > 0~COUNT:'):
            pass
        assert search_count[-1] == 16
        # remember to always get rid of the header
        new_values = [value[2] for value in new_instance.leer_datos_csv('"INDICE" > 10')][1:]
        assert new_values == ["Legacy of Ashes", "Whispering Winds", "Starlight Journey", "The Broken Crown", "Last of the Titans", "Skyfall Odyssey"]
        last_instance = SingleCsvManager("movies", current_class=writer_object, delimiter=",")
        last_instance.set_data(*['6', '2006', 'The Silent Hunter', '263068416']).guardar_datos_csv()
        for entry in last_instance.leer_datos_csv('"gross_income" >= 263068416'):
            pass
        assert entry[-1] == "263068416"
        assert entry[0] == "[13]"
        BaseCsvManager.delete_record("new_file", "movies")
        assert sum(1 for _ in BaseCsvManager.return_current_file_names()) == 0
        new_dir.rmdir()
        