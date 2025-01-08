from saveclass import CsvClassSave, DataExportError
from pathlib import Path
from time import sleep
import shutil
import csv
import hashlib
import dataclasses

try:
    # use pytest <file_to_test>.py to run 
    import pytest
except ImportError:
    raise ImportError("para ejecutar este test necesita instalar "
                      "el paquete pytest, use pip install pytest "
                      "para instalarlo, pagina paquete https://docs.pytest.org/en/stable/index.html")

data_test = Path(fr"{Path(__file__).parent}\data_test.csv")

def data_clean_up(single: bool, delimiter: str, data: Path, clean = False):
    if clean:
        if single:
            # on instance creation the backup and original get hash compared to
            # make the original equal to th backup
            CsvClassSave(str(data_test), None, single, delimiter)
        else:
            CsvClassSave(str(data_test), None, single, delimiter)
    with open(str(CsvClassSave.backup_single if single else CsvClassSave.backup_multi), "w", newline="", encoding="utf-8") as pass_data:
        data_writer = csv.writer(pass_data, delimiter=delimiter)
        with open(str(data), "r", newline="", encoding="utf-8") as has_data:
            read = csv.reader(has_data, delimiter=delimiter)
            for line in read:
                data_writer.writerow(line)


class TestBackUpIntegrity:
    """Engloba los test de creación y el mantenimiento de la persistencia de
    datos de los archivos csv usados como backup por el programa"""
    back_up = Path(fr"{Path(__file__).parent}\backup\backup_multi.csv"), Path(
        fr"{Path(__file__).parent}\backup\backup_single.csv")

    def test_creates_backup_directory(self):
        """Verificación de que si el directorio y los archivos de backup
        no existen que sean creados"""
        if (back_dir := Path(self.back_up[0].parent)).is_dir():
            shutil.rmtree(str(back_dir))
            sleep(2)
        with open(str(data_test), "w", encoding="utf-8") as _:
            pass
        CsvClassSave(str(data_test), single=False)
        CsvClassSave(str(data_test), single=True)
        sleep(2)
        assert self.back_up[0].is_file(), f"el archivo {self.back_up[0].name} no fue creado"
        CsvClassSave(str(data_test), single=True)
        sleep(2)
        assert self.back_up[1].is_file(), f"el archivo {self.back_up[1].name} no fue creado"

    def test_data_gets_rebuild_from_backup(self):
        """Prueba que en caso de que el archivo csv del usuario y su backup
        actual (el tipo usado dependerá de si el programa
        esta en modo single o no) difieran en su contenido se
        copien los contenidos del backup de vuelta al del usuario para
        que vuelvan a tener el mismo contenido"""
        rows = [["INDICE", "CLASE", "ATRIBUTOS"],
                ["[1]", "<class 'sudoku.Solution.SudokuSolution'>",
                 ("start: 11223344|||, end: 1234342121434312, "
                  "start_vals: 4, solving_time: 0.016, difficulty: 25, size: 4")],
                ["[2]", "<class 'sudoku.Solution.SudokuSolution'>",
                 "start: ---5-127995--863-4-74-92-8---945--2-735-29-6---8-6359786----74---327--56-27--4-3-, "
                 "end: 386541279952786314174392685619457823735829461248163597861935742493278156527614938, "
                 "start_vals: 45, solving_time: 0.0, difficulty: 37, size: 9"],
                ["[3]", "<class 'sudoku.Solution.SudokuSolution'>",
                 "start: 356c7b8eb2d9|1b5g7abdd1ef|4164dcf3|1c22538591f6|317g9aacdfge|2542e8g7|5b657fa8c2g9|"
                 "1e3d689bfa|6a748c9da1b7g8|"
                 "2c4a7281cbffg6|1d245889b3db|768f94agb9ecf7|2f325e96bgc9dae5f4g1|2g697dde|164d5faeb1d8|"
                 "7592bfc4edgg, end: 4d561cbef72"
                 "39g8ab3e7g6a289dc1f54g8f1947d56aecb32c2a93f8514bg7e6d39184dg6ac57f2bef5b2ce1a936d48g7"
                 "a7c4b5f3g8e261d9e6dg2897bf4153ac"
                 "2b635a4cd17fg9e89cga7321e58bd4f6d47f8ge9c236ba15518edb6f4g9a2c738f"
                 "2ce73b6dg9a5411g35a9d47bc8e62f6a4df2cg3e15879b7e9b61582af43dcg,"
                 " start_vals: 96, solving_time: 59.938, difficulty: 97934, size: 16"]]
        for val, option in zip(self.back_up, [False, True]):
            with open(str(data_test), "w", encoding="utf-8", newline="") as write:
                cont_w = csv.writer(write, delimiter="#")
                cont_w.writerows(rows)
            with open(str(val), "w", encoding="utf-8", newline="") as write:
                cont_w = csv.writer(write, delimiter="#")
                cont_w.writerows(rows)
            hash_list: list = []
            for files in (data_test, str(val)):
                hex_val = self.hash_file(str(files))
                if hex_val not in hash_list:
                    hash_list.append(hex_val)
            if len(hash_list) != 1:
                raise ValueError("HUBO UN PROBLEMA CON EL HASH DE LOS ARCHIVOS AL REALIZAR EL TEST")
            CsvClassSave(str(data_test), col_sep="#", single=option)
            with open(str(val), "w", encoding="utf-8", newline="") as write:
                cont_w = csv.writer(write, delimiter="#")
                cont_w.writerows(rows[0:2])
            # to see the change on the file
            sleep(2)
            CsvClassSave(str(data_test), col_sep="#", single=option)
            new_hash = self.hash_file(str(data_test))
            sleep(2)
            assert new_hash not in hash_list, "el hash del archivo principal no cambio al modificarse el backup"
        shutil.rmtree(str(self.back_up[0].parent))

    @staticmethod
    def hash_file(file: str) -> str:
        hash_file = hashlib.sha256()
        buffer_size = bytearray(128 * 1024)
        buffer_view = memoryview(buffer_size)
        with open(file, 'rb', buffering=0) as file_hash:
            for chunk_ in iter(lambda: file_hash.readinto(buffer_view), 0):
                hash_file.update(buffer_view[:chunk_])
        return hash_file.hexdigest()


class TestAttribute:
    """contiene los test para comprobar que solo se acepten los valores
    correctos a la hora de inicializar la clase CsvClassSave"""
    paths = 12, fr"{data_test.parent}\file_test.csv", fr"{data_test.parent}\test.txt"
    single_ = "False"
    col_sep_ = 0, "er"
    headers = (False, 
               ((), ("index",), ("index", "class"), ("index", "class", "attr", "other")), 
               ("index", "class", 3), (("INICIO", "inicio", "valor"), ("col", "col", "col")),)
    excludes = 15, (), ("wer", ["3"])
    hashed = "no"

    def test_path_invalid_type(self):
        """chequeando que si el tipo del atributo path_file no es str
        se lance un error"""
        with pytest.raises(ValueError, match="debe ser str"):
            CsvClassSave(self.paths[0])

    def test_path_not_present(self):
        """chequeando que si la ruta asociada al atributo path_file no existe
        se lance un error"""
        with pytest.raises(ValueError, match="no existe"):
            CsvClassSave(self.paths[1])

    def test_path_incorrect_ext(self):
        """chequeando que si la ruta asociada al atributo path_file no es de extension .csv
        se lance un error"""
        with pytest.raises(ValueError, match="de extension"):
            CsvClassSave(self.paths[2])

    def test_invalid_single(self):
        """chequeando que si el tipo requerido para el atributo single no es bool
        se lance un error"""
        with pytest.raises(ValueError, match="debe ser bool"):
            CsvClassSave(str(data_test), None, self.single_)

    def test_invalid_col_sep_type(self):
        """chequeando que si el tipo requerido para el atributo col_sep no es str
        se lance un error"""
        with pytest.raises(ValueError, match="debe ser str"):
            CsvClassSave(str(data_test), None, False, self.col_sep_[0])

    def test_invalid_col_sep_len(self):
        """chequeando que si el atributo col_sep contiene más de dos caracteres
        se lance un error"""
        with pytest.raises(ValueError, match="contener solo un carácter"):
            CsvClassSave(str(data_test), None, False, self.col_sep_[1])

    def test_invalid_header_type(self):
        """chequeando que si el tipo requerido para el atributo header no es tuple
        se lance un error"""
        with pytest.raises(ValueError, match="ser una tuple"):
            CsvClassSave(str(data_test), None, False, "-", self.headers[0])

    def test_invalid_header_with_less_than_3_str(self):
        """chequeando que si el atributo header no es una tuple con 3 str
        se lance un error"""
        with pytest.raises(ValueError, match="con 3 str"):
            for item in self.headers[1]:
                CsvClassSave(str(data_test), None, False, "-", item)

    def test_invalid_header_with_not_only_str(self):
        """chequeando que si el atributo header no es una tuple con solo str
        se lance un error"""
        with pytest.raises(ValueError, match="con 3 str"):
            CsvClassSave(str(data_test), None, False, "-", self.headers[2])

    def test_invalid_header_with_repeated_str_values(self):
        """se chequea que los valores pasados para el header sean todos distintos"""
        with pytest.raises(ValueError, match="con 3 str distintos"):
            for item in self.headers[3]:
                CsvClassSave(str(data_test), None, False, "-", item)

    def test_invalid_exclude_type(self):
        """chequeando que si el tipo del atributo exclude no es una tuple
        se lance un error"""
        with pytest.raises(ValueError, match="ser una tuple o None"):
            CsvClassSave(str(data_test), None, False, "-", ("indice", "clase", "parámetros"), self.excludes[0])

    def test_invalid_exclude_len(self):
        """chequeando que si el atributo exclude es una tuple vacía
        se lance un error"""
        with pytest.raises(ValueError, match="al menos un str"):
            CsvClassSave(str(data_test), None, False, "-", ("indice", "clase", "parámetros"), self.excludes[1])

    def test_invalid_exclude_content_type(self):
        """chequeando que si el atributo exclude contiene no solo str
        se lance un error"""
        with pytest.raises(ValueError, match="todos los valores de la tuple"):
            CsvClassSave(str(data_test), None, False, "-", ("indice", "clase", "parámetros"), self.excludes[2])

    def test_invalid_hashing_type(self):
        """chequeando que si el atributo check_hash no es bool
        se lance un error"""
        with pytest.raises(ValueError, match="debe ser bool"):
            CsvClassSave(str(data_test), None, False, ",", ("indice", "clase", "parámetros"), ("valores",), self.hashed)


# CsvClassSave on single mode
class TestSingle:
    """Engloba los test del programa cuando el mismo se ejecuta en modo single=True"""

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

    arguments = {"start": "-432-3---1-442-3",
                 "end": "1432234131244213",
                 "start_vals": 9,
                 "solving_time": 0.0,
                 "difficulty": 8,
                 "size": 4,
                 "other": "nada"
                 }

    case_data_single = Path(fr"{Path(__file__).parent}\data\data_single.csv")
    case_time_data = Path(fr"{Path(__file__).parent}\data\times.csv")
    case_empty_file = Path(fr"{Path(__file__).parent}\data\empty.csv")

    def test_static(self):
        """comprueba que el método estático retorne los resultados
        esperados (comportamiento no difiere en modo multi)"""
        for case in (True, 12, {"a": 5}, ["no", ], ("no",), set()):
            with pytest.raises(ValueError, match="debe ingresar un str"):
                CsvClassSave.return_pattern(case)
        assert CsvClassSave.return_pattern("[:3]") is None
        assert CsvClassSave.return_pattern("[3") is None
        assert CsvClassSave.return_pattern("palabra") is None
        assert CsvClassSave.return_pattern("[1-6-7-8-5-11-12-67-4-3-14]") is None
        assert CsvClassSave.return_pattern("[3:11]") == (":", [3, 11])
        assert CsvClassSave.return_pattern("[3:]") == (":", [3, ])
        assert CsvClassSave.return_pattern("[11:7]") == (":", [7, 11])
        assert CsvClassSave.return_pattern("[0:0]") == (":", [0, 0])
        assert CsvClassSave.return_pattern("[1-4-6]") == ("-", [1, 4, 6])
        assert CsvClassSave.return_pattern("[3-8-9-17-2]") == ("-", [2, 3, 8, 9, 17])
        assert CsvClassSave.return_pattern("[0-6-8]") == ("-", [0, 6, 8])
        assert CsvClassSave.return_pattern("[1-6-7-8-5-11-12-67-4-3]") == ("-", [1, 3, 4, 5, 6, 7, 8, 11, 12, 67])
        assert CsvClassSave.return_pattern("[0]") == (None, [0])
        assert CsvClassSave.return_pattern("[12]") == (None, [12])

    def test_class_method_index(self):
        """comprueba el funcionamiento del método de clase index para la
        importación de documentos csv"""
        # test for when you pass an empty .csv
        with pytest.raises(ValueError, match="No es posible realizar la operación en un archivo sin contenidos"):
            CsvClassSave.index(file_path=str(self.case_empty_file), delimiter="#")
        for invalid_arg, message in (((r"{data_test.parent}\test.txt", "#", True), "de extension"),
                                     ((None, "d", False), "debe ser str"),
                                     ((r"{data_test.parent}\test.csv", "<", True), "no existe"),
                                     ((str(data_test), "%", "no"), "argumento id_present"),
                                     ((str(data_test), "$$", False), "debe contener solo un")):
            with pytest.raises(ValueError, match=message):
                CsvClassSave.index(*invalid_arg)
        # probando que la importación tenga el formato deseado
        expected_head = ["INDICE", "NAME", "CITY", "AGE", "JOB", "DATE"]
        expected_vals = [f"[{i}]" for i in range(1, 12)]
        CsvClassSave.index(str(self.case_time_data), "#", False)
        get_values = []
        with open(fr"{Path(__file__).parent}\backup\backup_single.csv", "r", encoding="utf-8",
                  newline="") as class_method:
            csv_read = csv.reader(class_method, delimiter="#")
            assert expected_head == next(csv_read)
            for row in csv_read:
                get_values.append(row[0])
        assert get_values == expected_vals

    def test_single_seek(self):
        """comprueba que las búsquedas realizadas usando el modo single
        retorne los resultados esperados (algunos patrones de búsqueda son comunes
        a ambos modos)"""
        # this as a class variable gave a weird behavior
        # managing class state is painful in test
        # to create backup directory
        # cleaning data last test
        # cleaning data in data_test.csv data on the data_test
        data_clean_up(True, "#", self.case_data_single, True)
        single_test_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
        for invalid in (8, ["test"], False, {1, 7, 9}, {"test": "no valido"}):
            with pytest.raises(ValueError, match="argumento string_pattern debe ser str"):
                single_test_instance._CsvClassSave__query_parser(string_pattern=invalid)
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
            for item in single_test_instance.leer_datos_csv(query, back_up=True):
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
             '[START#END#SIZE] "START" ]= | | "END" ]= G~DESC:SIZE': [[5, 1, 4], 3, 4, head_6],
            }
        for exclude_query, values in special_query.items():
            collect_entries = []
            new_query = single_test_instance.leer_datos_csv(exclude_query, back_up=True)
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
        # all of this needs refeactoring
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
            query_func = single_test_instance.leer_datos_csv(functional_queries, back_up=True)
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
        CsvClassSave.index(file_path=str(self.case_time_data), delimiter="#", id_present=False)
        # test date data on search and operations, accepted format is ISO8601
        # repopulating with new data
        new_date_test = self.DateObjectTest("Sara Stew", "Cape Coral", "age", "Google Intern", "WWWWWWWWW")
        date_instance_test = CsvClassSave(str(data_test), new_date_test, True, "#")
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
                          # now unique automatically returns the count at the end
                          # the last number in the list inside the list
                          # is the total amount of entries for [[1, 5, 2], 3] 
                          # in [1, 5, 2] 2 is the amount of entries (1 and 5) 
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
            for dates in date_instance_test.leer_datos_csv(query_date, back_up=True):
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
        date_instance_test.guardar_datos_csv()
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
                         }
        for multi_type_col_query, str_val in str_date_test.items():
            get_entries = []
            for data in date_instance_test.leer_datos_csv(multi_type_col_query, back_up=True):
                if data[0] not in ("AVG", "MAX", "MIN", "SUM"):
                    get_entries.append(data[0])
                else:
                    get_entries.append(f"[{data[-1]}]")
            get_entries.pop(0)
            assert len(get_entries) == str_val[1], f"fallo en cantidad encontrada: {multi_type_col_query}"
            assert get_entries == [f"[{val}]" for val in str_val[0]], f"fallo en buscar entrada: {multi_type_col_query}"

    def test_single_update(self):
        """comprueba que solo sean actualizadas las filas requeridas por la consulta 
        y solo las columnas especificadas actualizar solo es posible en single=True"""
        CsvClassSave.index(file_path=str(self.case_time_data), delimiter="#", id_present=False)
        assert "Actualmente esta ocupando un objeto de tipo" in next(CsvClassSave(str(data_test), single=True,
                                                                                  col_sep="#").actualizar_datos(update_query="UPDATE"))
        update_date_test = self.DateObjectTest("Sara Stew", "Cape Coral", "30", "Google Intern", "2024-08-29")
        with pytest.raises(AttributeError, match="no es posible actualizar"):
            next(CsvClassSave(str(data_test), update_date_test, False, "#").actualizar_datos(update_query="UPDATE"))
        test_update_instance = CsvClassSave(str(data_test), update_date_test, True, "#")
        with pytest.raises(ValueError, match="debe ingresar un str"):
            # you have to use next in generators for the code to start executing
            next(test_update_instance.actualizar_datos(update_query={1, 4, 6}))
        # TODO TEST %NUM-FORMAT
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
        CsvClassSave.index(file_path=str(self.case_time_data), delimiter="#", id_present=False)
        update_test_two = CsvClassSave(str(data_test), update_date_test, True, "#")
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
        rows = update_test_two.leer_datos_csv('"JOB" =  ', back_up=True)
        next(rows)
        assert next(rows)[0] == "[2]", "fallo en actualizar y buscar 2"
        assert next(rows)[0] == "[11]", "fallo en actualizar y buscar 11"  


    def test_index_write(self):
        """comprueba que se cree una clase nueva cuando se pasa un csv que no depende de
        objetos de python y que la clase creada permita agregar y actualizar valores"""
        write_object = CsvClassSave.index(file_path=str(self.case_time_data), delimiter="#", id_present=False)
        write_instance = write_object(**{"name": "Finn", "city": "Port Vila", 
                                         "age": "25", "job": "Developer", "date": "1999-08-17"})
        test_instance = CsvClassSave(path_file=str(data_test), class_object=write_instance, single=True, col_sep="#")
        new_entry = test_instance.guardar_datos_csv()
        assert new_entry == "\nINDICE#NAME#CITY#AGE#JOB#DATE\n[12]#Finn#Port Vila#25#Developer#1999-08-17", f"error al crear entrada {new_entry}"
        updated_entry = test_instance.actualizar_datos(update_query='UPDATE:~"NAME"=Jake "AGE"=35 ON "INDICE" = 12')
        expected = next(updated_entry)["result"] 
        assert expected == "[12]#Jake#Port Vila#35#Developer#1999-08-17".split("#"), f"error al actualizar entrada {'#'.join(expected)}"

        invalid_combinations = [(set(), []), ((1,), "HOLA"), 
                                ({}, 12.6), ([], []), ({}, {})]
        for extra, excluded in invalid_combinations:
            with pytest.raises(ValueError, match="pero su argumento fue de tipo"):
                CsvClassSave.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, extra_columns=extra, exclude=excluded)

        # the program first exclude the cols and then add the new ones
        # so this ({"UNIVERSITY": "MIT", "JOB": " "}, ["JOB", "NAME"]) does not
        # trow an error
        invalid_new_col_names = [({"SALARY": "", "NAME": "NULL"}, None),
                                 ({"SALARY": "", "salary": "NULL"}, ["name"])]
        for new_col, removed_col in invalid_new_col_names:
            with pytest.raises(ValueError, match="los encabezados no pueden tener nombres repetidos para las columnas"):
                CsvClassSave.index(file_path=str(self.case_time_data), delimiter="#", id_present=False, extra_columns=new_col, exclude=removed_col)


        all_exclude_col_object = CsvClassSave.index(file_path=str(self.case_time_data), 
                                              delimiter="#", id_present=False, exclude=["INDICE", "NAME", "Job", "DATE", "city", "aGe"])
        test_instance_2 = CsvClassSave(path_file=str(data_test), class_object=all_exclude_col_object(), single=True, col_sep="#")

        result_instance_2 = test_instance_2.leer_datos_csv()
        assert next(result_instance_2) == ["INDICE"], "fallo en encabezado"
        assert len(next(result_instance_2)) == 1, "fallo en cantidad esperada"

        added_col_object = CsvClassSave.index(file_path=str(self.case_time_data), 
                                              delimiter="#", id_present=False, exclude=["city",], extra_columns={"SALARY": "", 
                                                                                                                 "TAX": "25",
                                                                                                                 "TOTAL": "0", 
                                                                                                                 "CITY": " "})
        new_col_object = added_col_object(**{"name": "Lian", 
                                             "age": 31,
                                             "job": "DevOps",
                                             "date": "2016-07-24",
                                             "salary": 2500,
                                             "tax": 15,
                                             "total": 2500 - 2500 * 0.15,
                                             "city": "Ohio",})
        test_instance_3 = CsvClassSave(path_file=str(data_test), class_object=new_col_object, single=True, col_sep="#")
        for value in test_instance_3.leer_datos_csv(search='[SALARY#CITY] "INDICE" > 0~UNIQUE:SALARY'):
            # for this case with unique only one result is expected
            if value[0] not in ("UNIQUE", "INDICE"):
                assert value[0] in [f"[{i}]" for i in range(1, 2)], f"fallo en cantidad encontrada {value}"
                assert all([val == "VOID" for val in value[1:]])
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
        write_object_with_index = CsvClassSave.index(file_path=str(self.case_data_single), delimiter="#", 
                                                     extra_columns={"time_difficulty_ratio": ""}, exclude=["start", "start", "end",])
        index_instance = write_object_with_index(**{"start_vals": 90, "solving_time": 603.60, "difficulty": 736425, "size": 16, "time_difficulty_ratio": 0.000820})
        test_instance_indexed = CsvClassSave(path_file=str(data_test), class_object=index_instance, single=True, col_sep="#")
        test_result = [0.16, 1.216, 0.286, 0.308, 0.001, 0.0, 0.011, 0.047, 0.03, 0.015, 0.035, 0.608, 0.176, 
                       0.267, 0.099, 0.261, 0.087, 0.067, 0.029, 0.014]
        update_query = ('UPDATE:~"time_difficulty_ratio"=%COPY:~start_vals "time_difficulty_ratio"=%DIV:~USE:~difficulty '
                       '"time_difficulty_ratio"=%NUM-FORMAT:~3 ON "INDICE" > 0')
        for next_test, next_result in zip(test_instance_indexed.actualizar_datos(update_query=update_query), test_result, strict=True):
            assert next_test["result"][-1] == f"{next_result:.3f}"

    def test_single_delete(self):
        """comprueba que se borren las entradas especificadas, el comportamiento
        es igual independiente del modo (single (single=True) o multiple (single=False))
        para las consultas que no usen DELETE ON (solo modo single=True)"""
        # clean up the data always before an assert
        # putting data back on track
        # in case a previous test fails
        data_clean_up(True, "#", self.case_data_single)
        test_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
        invalid_delete = ["hola", "size=9", "[:12]", "[1-12-14-15-6-7-9-10-11-2-14]", "[10-]",
                          "<class 'vehiculo.Bicicleta'>",
                          'DELETE ON[solving_time#difficulty#size] "difficulty" > 10 & "difficulty" < 100',]
        for query in invalid_delete:
            with pytest.raises(ValueError, match="o escribiendo una consulta usando la palabra clave DELETE"):
                next(test_instance.borrar_datos(query))

        for bad_arg in ((12, True), ("[2:5]", 13), (True, "[2:5]")):
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
        data_clean_up(True, "#", self.case_data_single)
        # overriding old instance to sync original to backup
        test_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
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
        """chequea que se de una advertencia si se intenta guardar un objeto que no
        soporte __dict__ ya que es lo que se usa para guardar los atributos"""
        # data restoration before the next test is run
        data_clean_up(True, "#", self.case_data_single)
        assert "Actualmente esta ocupando un objeto de tipo" in CsvClassSave(str(self.case_data_single), single=True,
                                                                             col_sep="#").guardar_datos_csv()

    def test_max_row_zero(self):
        """chequea que si se excede la capacidad designada para la cantidad de filas
        se retorne una advertencia al usuario al tratar de crear una nueva entrada"""

        # test class so the object has __dict__
        class RowsTest:
            filler = 0

        test_instance = CsvClassSave(str(data_test), class_object=RowsTest(), single=True, col_sep="#")
        test_instance.max_row_limit = 0
        assert "Advertencia: Su entrada no fue creada" in test_instance.guardar_datos_csv()
        test_instance.max_row_limit = 13
        # plus one to not count header
        test_instance.current_rows = 14
        assert "Advertencia: Su entrada no fue creada" in test_instance.guardar_datos_csv()

    def test_not_matched_dict_object_single(self):
        """en el modo single no se pueden escribir nuevas entrada que no tengan los mismo nombres
        de atributos y cantidad de atributos que las entradas ya presentes aquí se chequea que
        si al pasar un objeto que no calza con los ya presentes se lance un error"""
        new_local_object = self.SingleObjectTest(**self.arguments)
        new_instance = CsvClassSave(str(data_test), new_local_object, True, col_sep="#")
        # length inequality
        with pytest.raises(ValueError, match="en modo single = True"):
            # if you use this program you shouldn't change from
            # single True to False if this module is already being used for
            # on a mode it will only create bugs
            new_instance.guardar_datos_csv()

        # name inequality
        new_instance.exclude = ("size",)
        with pytest.raises(ValueError, match="en modo single = True"):
            # if you use this program you shouldn't change from
            # single True to False if this module is already being used for
            # on a mode it will only create bugs
            new_instance.guardar_datos_csv()

    def test_exclude_single(self):
        """comprueba que los atributos del objeto excluidos usando el
        atributo exclude sean excluidos al escribir una nueva entrada ('!' al
        inicio de exclude es para negar y que solo se incluya lo después del '!')"""
        local_instance = CsvClassSave(str(data_test), self.SingleObjectTest(**self.arguments), True, "#",
                                      exclude=("other",))
        assert local_instance.guardar_datos_csv() == ("\nINDICE#START#END#START_VALS#SOLVING_TIME#DIFFICULTY#"
                                                      "SIZE\n[21]#-432-3---1-442-3#1432234131244213#9#0.0#8#4")
        local_instance.object.end = ":" + local_instance.object.end
        # comprobando que en modo single = True los : no son remplazados
        assert local_instance.guardar_datos_csv() == ("\nINDICE#START#END#START_VALS#SOLVING_TIME"
                                                      "#DIFFICULTY#SIZE\n[22]#-432-3---1-442-3#:"
                                                      "1432234131244213#9#0.0#8#4")
        local_instance.exclude = ("!", "other")
        # esto lanza error (como debería) por que es modo single y la cantidad de atributos
        # no calza al negar exclude con los de las entradas ya escritas
        with pytest.raises(ValueError, match="en modo single = True"):
            local_instance.guardar_datos_csv()

    def test_enforce_single(self):
        """comprueba que se pasen los argumentos apropiados al método guardar_datos_csv
        y que al pasar los correctos se aplique la función de enforce_unique que es
        comprobar que solo se puedan escribir nuevas entradas con valores únicos para los
        campos pasados al argumento enforce_unique"""
        local_instance = CsvClassSave(str(data_test), self.SingleObjectTest(**self.arguments), True, "#",
                                      exclude=("other",))
        # cleaning data always before the next assert
        for _ in local_instance.borrar_datos("[21:]"):
            pass
        for cases, message in {12: "debe ser una tuple", (): "al menos un str",
                               (True,): "solo debe contener str", ("size", 12): "solo debe contener str"}.items():
            with pytest.raises(ValueError, match=message):
                local_instance.guardar_datos_csv(enforce_unique=cases)
        assert local_instance.guardar_datos_csv(("size",)) == "presente"
        assert local_instance.guardar_datos_csv(("size", "solving_time")) == "presente"

    def test_export_single(self):
        """comprueba que al querer usar export en modo single = True se lance un
        error ya que no se encuentra disponible el método con esa opción"""
        test_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
        with pytest.raises(AttributeError, match="no disponible en modo single = True"):
            test_instance.export("", "")


class TestMultiple:
    """Engloba los test para el modo multiple o single = False,
    solo implementando pruebas en aquellos lugares donde ambos modos
    difieren en la ejecución de su código"""
    case_data_multiple = Path(fr"{Path(__file__).parent}\data\data_multiple_class.csv")

    @dataclasses.dataclass
    class MultiObjectTest:
        nombre: str
        edad: int
        nacimiento: int
        estado: str
        comentario: str

    arguments = {"nombre": "Mark Test",
                 "edad": 40,
                 "nacimiento": 1984,
                 "estado": "soltero",
                 "comentario": "estoy tranquilo"}

    def test_seek_multiple(self):
        """comprobando que la búsqueda de entradas funcione como es debido
        en modo multi o single=False (en general que de resultados distintos en
        ciertas búsquedas con respecto a los que da en modo single)"""
        data_clean_up(False, "|", self.case_data_multiple)
        multi_test_instance = CsvClassSave(str(data_test), single=False, col_sep="|")
        # en modo multiple no hay soporte para operadores como el or y and
        # del modo simple para buscar, aparte de eso la funcionalidad
        # para buscar es la misma entre los dos modos (en modo multiple
        # para buscar por atributo (columna en modo single) usas
        # nombre_columna: valor)
        search = {'"marca" = Ford & velocidad = 180': [[], 0], "marca: Ford & velocidad: 180": [[], 0],
                  '"marca" = ford | velocidad >= 180': [[], 0], "marca: Ford": [[1, 11], 2],
                  "marca: ford": [[1, 11], 2],
                  "vehiculo.automovil": [[5, 10, 11], 3]}
        for query, value in search.items():
            collect_entries = []
            for item in multi_test_instance.leer_datos_csv(query, back_up=True):
                collect_entries.append(item[0])
            collect_entries.pop(0)
            assert len(collect_entries) == value[1], f"fallo en cantidad encontrada: {query}"
            assert collect_entries == [f"[{val}]" for val in value[0]], f"fallo en buscar entrada: {query}"
        with pytest.raises(AttributeError, match="filtrado por selector lógico no disponible"):
            # access private method
            multi_test_instance._CsvClassSave__query_parser(string_pattern='"marca" = Ford & velocidad = 180')

    def test_delete_multi(self):
        """comprueba que en modo multiple se pueda borrar entradas usando el nombre de una clase"""
        local_instance = CsvClassSave(str(data_test), single=False, col_sep="|")
        assert local_instance.current_classes == ["<class 'vehiculo.Particular'>", "<class 'vehiculo.Carga'>",
                                                  "<class 'vehiculo.Bicicleta'>", "<class 'vehiculo.Motocicleta'>",
                                                  "<class 'vehiculo.Automovil'>"]
        with pytest.raises(ValueError, match="introduciendo el nombre completo"):
                next(local_instance.borrar_datos('DELETE ON "INDICE" > 5'))
        deleted_entries = []
        for item in local_instance.borrar_datos(delete_index="<class 'vehiculo.Particular'>"):
            deleted_entries.append(str(item).split("|")[0])
        deleted_entries.pop(0)
        assert deleted_entries == ["[1]", "[9]"]
        assert local_instance.current_classes == ["<class 'vehiculo.Carga'>", "<class 'vehiculo.Bicicleta'>",
                                                  "<class 'vehiculo.Motocicleta'>", "<class 'vehiculo.Automovil'>"]

    def test_not_matched_dict_object_multiple(self):
        """comprobando que en modo multiple se puedan guardar objetos con distinto
        número y nombre de atributos (esta es la función que implementa el modo multiple a
        diferencia del modo single donde solo un tipo de objetos de igual numero y nombre
        de atributos se debe guardar)
        """
        # restoring last test data changes
        data_clean_up(False, "|", self.case_data_multiple)
        local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**self.arguments), False, "|")
        has_to_be = ("\nINDICE|CLASE|ATRIBUTOS\n[12]|"
                     "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                     "nombre: Mark Test, edad: 40, nacimiento: 1984, estado: soltero, comentario: estoy tranquilo")
        assert local_instance.guardar_datos_csv() == has_to_be

    def test_exclude_multiple(self):
        """comprueba que los atributos del objeto excluidos usando el
        atributo exclude sean excluidos al escribir una nueva entrada en modo
        multiple ya que hay diferencias en el código usado para hacerlo dependiendo del modo"""
        local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**self.arguments), False, "|",
                                      exclude=("comentario",))
        for _ in local_instance.borrar_datos("[12]"):
            pass
        has_to_be_1 = ("\nINDICE|CLASE|ATRIBUTOS\n[12]|"
                       "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                       "nombre: Mark Test, edad: 40, nacimiento: 1984, estado: soltero")
        assert local_instance.guardar_datos_csv() == has_to_be_1
        local_instance.exclude = ("!", "comentario")
        has_to_be_2 = ("\nINDICE|CLASE|ATRIBUTOS\n[13]|"
                       "<class 'test_csv.TestMultiple.MultiObjectTest'>|comentario: estoy tranquilo")
        assert local_instance.guardar_datos_csv() == has_to_be_2
        local_instance.exclude = ("nombre", "nacimiento", "estado")
        has_to_be_3 = ("\nINDICE|CLASE|ATRIBUTOS\n[14]|"
                       "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                       "edad: 40, comentario: estoy tranquilo")
        assert local_instance.guardar_datos_csv() == has_to_be_3

    def test_enforce_multiple(self):
        """comprueba el funcionamiento del argumento enforce_unique
        del método guardar_datos_csv en modo multiple"""
        local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**self.arguments), False, "|")
        for _ in local_instance.borrar_datos("[12:]"):
            pass
        local_instance.guardar_datos_csv()
        assert local_instance.guardar_datos_csv(enforce_unique=("comentario",)) == "presente"
        local_argument = {"nombre": "Mark mayz",
                          "edad": 40,
                          "nacimiento": 1984,
                          "estado": "casado",
                          "comentario": "nada"}
        new_local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**local_argument), False, "|")
        assert new_local_instance.guardar_datos_csv(enforce_unique=("nacimiento", "edad")) == "presente"
        has_to_be = ("\nINDICE|CLASE|ATRIBUTOS\n[13]|"
                     "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                     "nombre: Mark mayz, edad: 40, nacimiento: 1984, estado: casado, comentario: nada")
        # all enforce unique fields have to be present for the filter to work
        assert new_local_instance.guardar_datos_csv(enforce_unique=("estado", "nombre", "nacimiento")) == has_to_be

    def test_remove_invalid_char(self):
        """comprueba la correcta eliminación y remplazo del carácter : (modo single = False solamente)
        el cual es ocupado para separar valores dentro de la columna atributos
        con los cuales luego se puede exportar datos usando el método export"""
        local_argument = {"nombre": "Mark : mayz",
                          "edad": 40,
                          "nacimiento": 1984,
                          "estado": "ca:sado:",
                          "comentario": "::nada de lo que esta aquí me gustaría decirlo, asi que adios: me despido"}
        new_local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**local_argument), False, col_sep="|")
        for _ in new_local_instance.borrar_datos("[12:]"):
            pass
        expected_str = ("\nINDICE|CLASE|ATRIBUTOS\n[12]|"
                        "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                        "nombre: Mark ; mayz, edad: 40, nacimiento: 1984, estado: ca;sado;, "
                        "comentario: ;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido")
        expected_str_2 = ("\nINDICE|CLASE|ATRIBUTOS\n[13]|"
                          "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                          "nombre: Mark ; mayz, nacimiento: 1984, estado: ca;sado;, "
                          "comentario: ;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido")
        expected_str_3 = ("\nINDICE|CLASE|ATRIBUTOS\n[14]|"
                          "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                          "nombre: Mark ; mayz, nacimiento: 1960, estado: ca;sado;, "
                          "comentario: ;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido")
        assert new_local_instance.guardar_datos_csv() == expected_str
        new_local_instance.exclude = ("edad",)
        assert new_local_instance.guardar_datos_csv() == expected_str_2
        # enforce unique comes before exclude
        local_argument["nacimiento"] = 1960
        new_local_instance.object = self.MultiObjectTest(**local_argument)
        assert new_local_instance.guardar_datos_csv(enforce_unique=("nacimiento",)) == expected_str_3

    def test_export_multi(self):
        """comprueba la correcta exportación de datos almacenados en modo
        single = False para la creación de csv individuales a partir de un
        conjunto de clases"""
        destination = Path(fr"{Path(__file__).parent}\data\export_destination.csv")
        new_local_instance = CsvClassSave(str(data_test), None, False, col_sep="|", check_hash=False)
        for _ in new_local_instance.borrar_datos("[14]"):
            pass
        incorrect_args = [((fr"{destination.parent}\destino.csv", "vehiculo"), "de destino debe ser una ruta valida"),
                          ((fr"{data_test.parent}\file_test.txt", "vehiculo"), "debe ser de extension .csv"),
                          ((12, "<class 'vehiculo.Automovil'>"), "el argumento destination debe ser str"),
                          ((str(destination), True), "class_name debe ser str")]
        for value, match in incorrect_args:
            with pytest.raises(ValueError, match=match):
                new_local_instance.export(*value)

        with pytest.raises(DataExportError, match="cantidad de atributos no corresponde"):
            # if you use enforce unique you can end with two
            # rows of a same class header that have an uneven
            # amount of attributes and you end with a DataExportError
            new_local_instance.export(str(destination), "<class 'test_csv.TestMultiple.MultiObjectTest'>")
        final_state = ("INDICE|MARCA|MODELO|RUEDAS|VELOCIDAD|CILINDRADA\n"
                       "[1]|Toyota|Yaris|4|200|1200\n"
                       "[2]|nissan|360z|4|310|3000\n"
                       "[3]|ford|skype|4|180|3000\n")
        new_local_instance.export(str(destination), "<class 'vehiculo.Automovil'>")
        # newline not really necessary if you are going to read only
        contents = []
        with open(str(destination), "r", newline="", encoding="utf-8") as new_reader:
            read_result = csv.reader(new_reader, delimiter="|")
            for result in read_result:
                contents.append("|".join(result) + "\n")
        assert final_state == "".join(contents)
        for _ in new_local_instance.borrar_datos("[13]"):
            pass
        # doing second test
        new_local_instance.export(str(destination), "<class 'test_csv.TestMultiple.MultiObjectTest'>")
        final_state = ("INDICE|NOMBRE|EDAD|NACIMIENTO|ESTADO|COMENTARIO\n"
                       "[1]|Mark ; mayz|40|1984|ca;sado;|;;nada de lo que esta aquí me gustaría decirlo, "
                       "asi que adios; me despido\n")
        contents.clear()
        with open(str(destination), "r", newline="", encoding="utf-8") as new_reader_2:
            read_result_2 = csv.reader(new_reader_2, delimiter="|")
            for result_2 in read_result_2:
                contents.append("|".join(result_2) + "\n")
        assert final_state == "".join(contents)
        # third test
        for _ in new_local_instance.borrar_datos("borrar todo"):
            pass
        final_state = ('INDICE;NOMBRE;EDAD;NACIMIENTO;ESTADO;COMENTARIO'
                       '[1];"aq;uí";0;17;";ahora;";HOLA')
        new_local_instance.delimiter = ";"
        # it was false because the original object had no __dict__
        new_local_instance.can_save = True
        new_local_instance.object = self.MultiObjectTest(
            **{"nombre": "aq;uí", "edad": 0, "nacimiento": 17, "estado": ";ahora;", "comentario": "HOLA"})
        new_local_instance.guardar_datos_csv()
        new_local_instance.export(str(destination), "<class 'test_csv.TestMultiple.MultiObjectTest'>")
        # using normal reader because csv reader get rid of the ""
        # when reading them an joining the result of the list
        # you still can see how the file is left after the test
        # to see that it does have the "" on them
        accumulate = ""
        with open(str(destination), "r", newline="", encoding="utf-8") as new_reader_3:
            for result_3 in new_reader_3:
                # get rid of the \r\n that was in the file
                accumulate += result_3.strip()
        assert final_state == accumulate
