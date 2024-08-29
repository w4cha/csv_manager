from saveclass import CsvClassSave, DataExportError
from pathlib import Path
from time import sleep
import shutil
import csv
import hashlib
import dataclasses
try:
    import pytest
except ImportError:
    raise ImportError("para ejecutar este test necesita instalar "
                      "el paquete pytest, use pip install pytest "
                      "para instalarlo, pagina paquete https://docs.pytest.org/en/stable/index.html")

data_test = Path(fr"{Path(__file__).parent}\data_test.csv")

class TestBackUpIntegrity:
    """Engloba los test de creación y el mantenimiento de la persistencia de
    datos de los archivos csv usados como backup por el programa"""
    back_up = Path(fr"{Path(__file__).parent}\backup\backup_multi.csv"), Path(fr"{Path(__file__).parent}\backup\backup_single.csv")

    def test_creates_backup_directory(self):
        """Verificación de que si el directorio y los archivos de backup 
        no existen que sean creados"""
        if (back_dir:=Path(self.back_up[0].parent)).is_dir():
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
        """Prueba que en caso de que el archivo csv del usuario y su backup actual (el tipo usado dependerá de si el programa
        esta en modo single o no) difieran en su contenido se copien los contenidos del backup de vuelta al del usuario para
        que vuelvan a tener el mismo contenido"""
        rows = [["INDICE", "CLASE", "ATRIBUTOS"],
                    ["[1]", "<class 'sudoku.Solution.SudokuSolution'>", 
                    "start: 11223344|||, end: 1234342121434312, start_vals: 4, solving_time: 0.016, difficulty: 25, size: 4"],
                    ["[2]", "<class 'sudoku.Solution.SudokuSolution'>", 
                    "start: ---5-127995--863-4-74-92-8---945--2-735-29-6---8-6359786----74---327--56-27--4-3-, "
                    "end: 386541279952786314174392685619457823735829461248163597861935742493278156527614938, start_vals: 45, solving_time: 0.0, difficulty: 37, size: 9"],
                    ["[3]", "<class 'sudoku.Solution.SudokuSolution'>", 
                    "start: 356c7b8eb2d9|1b5g7abdd1ef|4164dcf3|1c22538591f6|317g9aacdfge|2542e8g7|5b657fa8c2g9|1e3d689bfa|6a748c9da1b7g8|"
                    "2c4a7281cbffg6|1d245889b3db|768f94agb9ecf7|2f325e96bgc9dae5f4g1|2g697dde|164d5faeb1d8|7592bfc4edgg, end: 4d561cbef72"
                    "39g8ab3e7g6a289dc1f54g8f1947d56aecb32c2a93f8514bg7e6d39184dg6ac57f2bef5b2ce1a936d48g7a7c4b5f3g8e261d9e6dg2897bf4153ac"
                    "2b635a4cd17fg9e89cga7321e58bd4f6d47f8ge9c236ba15518edb6f4g9a2c738f2ce73b6dg9a5411g35a9d47bc8e62f6a4df2cg3e15879b7e9b61582af43dcg,"
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
        hash_file: hashlib._Hash  = hashlib.sha256()
        buffer_size  = bytearray(128*1024)
        buffer_view = memoryview(buffer_size)
        with open(file, 'rb', buffering=0) as file_hash:
            for chunk_ in iter(lambda : file_hash.readinto(buffer_view), 0):
                hash_file.update(buffer_view[:chunk_])
        return hash_file.hexdigest()


class TestAttribute:
    """contiene los test para comprobar que solo se acepten los valores
    correctos a la hora de inicializar la clase CsvClassSave"""
    paths = 12, fr"{data_test.parent}\file_test.csv", fr"{data_test.parent}\test.txt"
    single_ = "False"
    col_sep_ = 0, "er"
    headers = False, ((), ("index",), ("index", "class"), ("index", "class", "attr", "other")), ("index", "class", 3)
    excludes = 15, (), ("wer", ["3"])

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

    arguments = {"start": "-432-3---1-442-3",
                 "end": "1432234131244213",
                 "start_vals": 9,
                 "solving_time": 0.0,
                 "difficulty": 8,
                 "size": 4,
                 "other": "nada"
                }

    case_data_single = Path(fr"{Path(__file__).parent}\data\data_single.csv")

    def test_static(self):
        """comprueba que el método estático retorne los resultados 
        esperados (comportamiento no difiere en modo multi)"""
        for case in (True, 12, {"a": 5}, ["no",], ("no",), set()):
            with pytest.raises(ValueError, match="debe ingresar un str"):
                CsvClassSave.return_pattern(case)
        assert CsvClassSave.return_pattern("[:3]") == None
        assert CsvClassSave.return_pattern("[3") == None
        assert CsvClassSave.return_pattern("palabra") == None
        assert CsvClassSave.return_pattern("[1-6-7-8-5-11-12-67-4-3-14]") == None
        assert CsvClassSave.return_pattern("[3:11]") == (":", [3, 11])
        assert CsvClassSave.return_pattern("[3:]") == (":", [3,])
        assert CsvClassSave.return_pattern("[11:7]") == (":", [7, 11])
        assert CsvClassSave.return_pattern("[0:0]") == (":", [0, 0])
        assert CsvClassSave.return_pattern("[1-4-6]") == ("-", [1, 4, 6])
        assert CsvClassSave.return_pattern("[3-8-9-17-2]") == ("-", [2, 3, 8, 9, 17])
        assert CsvClassSave.return_pattern("[0-6-8]") == ("-", [0, 6, 8])
        assert CsvClassSave.return_pattern("[1-6-7-8-5-11-12-67-4-3]") == ("-", [1, 3, 4, 5, 6, 7, 8, 11, 12, 67])
        assert CsvClassSave.return_pattern("[0]") == (None, [0])
        assert CsvClassSave.return_pattern("[12]") == (None, [12])

    def test_single_seek(self):
        """comprueba que las búsquedas realizadas usando el modo single
        retorne los resultados esperados (algunos patrones de búsqueda son comunes
        a ambos modos)"""
        # this as a class variable gave a weird behavior
        # managing class state is painful in test
        create_path_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
        with open(str(create_path_instance.backup_single), "w", newline="", encoding="utf-8") as pass_data:
            data_writer = csv.writer(pass_data, delimiter="#")
            with open(str(self.case_data_single), "r", newline="", encoding="utf-8") as has_data:
                read = csv.reader(has_data, delimiter="#")
                for line in read:
                    data_writer.writerow(line)
        single_test_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
        for invalid in (8, ["test"], False, {1, 7, 9}, {"test": "no valido"}):
            with pytest.raises(ValueError, match="argumento string_pattern debe ser str"):
                single_test_instance._CsvClassSave__query_parser(string_pattern=invalid)
        for bad_type in ((True, "test", False), (12, True, "test"), ("[10:]", 12, 15), ("[3]", True, 2.5)):
            with pytest.raises(ValueError, match="el argumento"):
                next(single_test_instance.leer_datos_csv(*bad_type))
        # single seek valid parameters
        search = {"[2:]": [list(range(2,21)), 19], "[1:4]": [list(range(1,5)), 4], "[11]": [[11], 1],
                  "[0:0]": [[], 0], "[0-5]": [[5], 1], "[2-12-1-7-18]": [[1, 2, 7, 12, 18], 5],
                  "[1-12-14-15-6-7-9-10-11-2]": [[1, 2, 6, 7, 9, 10, 11, 12, 14, 15], 10], 
                  "[25]": [[], 0], "[0:5]": [list(range(1,6)), 5],
                                                   # and operator
                   '"size" = 16': [[5, 8, 10, 20], 4], '"size" = 9 & "solving_time" = 0.0': [[2, 12], 2],
                   '"size" = 9 & "solving_time = 0.0': [[], 0], '"size" = 9 "solving_time" = 0.0': [[], 0],
                   '"size" = 9 & "solving_time": 0.0': [[], 0], '"size" = 9 % "solving_time" = 0.0': [[], 0],
                   '"size" = 9 & "solving_time" = 0 & "difficulty" = 51': [[12], 1], 
                   '"size" = 9 & "solving_time" = 0.0 | "difficulty" = 51': [[2, 12], 2], 
                   '"size"=4|"size"=9': [[], 0], '"size" = 4 | "size" = 9 | "size" = 16': [list(range(1,21)), 20],
                   '"size" = 9 | "solving_time" = 0': [[1, 2, 3, 4, 6, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19], 16],
                   '"size" = 4 & "size" = 9 & "size" = 16': [[], 0], '"size" = 4 & "size" = 9 | "size" = 16': [[5, 8, 10, 20], 4],
                   '"size" < 4': [[], 0], '"size" <= 4': [[1, 3, 4], 3], '"size" != 4': [[2] + list(range(5,21)), 17],
                   '"size" > 4': [[2] + list(range(5,21)), 17], '"size" => 4': [[], 0], '"size" >= 16': [[5, 8, 10, 20], 4],
                   '"size" > 0': [list(range(1,21)), 20], '"size" < 100': [list(range(1,21)), 20], '"size" > test': [[], 0],
                   '"size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3],
                   '"difficulty" < 6800 & "difficulty" >= 5000 | "size" < 4 & "solving_time" != 4.047': [[20], 1],
                   # if you chain more than 4 the last ones get combined into one "solving_time" != (4.047 & "size" < 20)
                   '"size" > 4 & "difficulty" >= 6000 & "difficulty" < 7000 & "solving_time" != 4.047 & "size" < 20': [[10, 20], 2],
                   # checking if old system is recognized
                   "size=16": [[], 0], "size=9~solving_time=0.0": [[], 0], 
                   "size=9~solving_time=0.0~difficulty=51": [[], 0], "size=9~solving_time=0.0 difficulty=51": [[], 0],
                   "size=9~solving_time=0.0 solving_time=0.312": [[], 0], "size=9 solving_time=0.0": [[], 0], 
                   "size=10": [[], 0], "1b5": [[5, 20], 2], "": [list(range(1,21)), 20], "size: 9": [[], 0]}
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
        special_query = {'"size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 7, head_1],
                         '![] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 7, head_1],
                         '[] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 7, head_1],
                         # space inside [] is not permitted
                         '![size  ] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[], 0, 7, head_1],
                         '![indice] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 7, head_1],
                         '![start_] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 7, head_1],
                         '![START#END] "size" > 4': [[2] + list(range(5,21)), 17, 5, head_2],
                         '[START#END] "size" > 4': [[2] + list(range(5,21)), 17, 3, head_4],
                         '![START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 5, head_2],
                         '[START#END] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 3, head_4],
                         '![START#END#start_vals] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 4, head_3],
                         '[START#END#start_vals] "size" > 4 & "difficulty" >= 5000 | "difficulty" > 6700 & "solving_time" != 4.047': [[5, 6, 20], 3, 4, head_5]
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
 
    def test_single_delete(self):
            """comprueba que se borren las entradas especificadas, el comportamiento
            es igual independiente del modo (single (single=True) or multiple (single=False))"""
            test_instance = CsvClassSave(str(data_test), single=True, col_sep="#")
            invalid_delete = ["hola", "size=9", "[:12]", "[1-12-14-15-6-7-9-10-11-2-14]", "[10-]", "<class 'vehiculo.Bicicleta'>"]
            for query in invalid_delete:
                with pytest.raises(ValueError, match="uno de los siguientes formatos"):
                    next(test_instance.borrar_datos(query))
        
            for bad_arg in ((12, True), ("[2:5]", 13), (True, "[2:5]")):
                with pytest.raises(ValueError, match="el argumento"):
                    next(test_instance.borrar_datos(*bad_arg))
        
            # borrar dato con indice inexistente
            with pytest.raises(ValueError, match="ninguno de los valores"):
                next(test_instance.borrar_datos("[25]"))
    
            valid_delete = {"[1]": [[1], 19], "[2:4]": [list(range(2,5)), 16], 
                            "[1-5-10]": [[1, 5, 10], 13], "[3:]": [list(range(3, 14)), 2]}
            for deleted, value in valid_delete.items():
                collect_deleted = []
                for item in test_instance.borrar_datos(deleted):
                    collect_deleted.append(str(item).split("#")[0])
                collect_deleted.pop(0)
                assert test_instance.current_rows - 1 == value[1], f"fallo en cantidad borrada: {deleted}"
                assert collect_deleted == [f"[{val}]" for val in value[0]], f"fallo en borrar entrada: {deleted}"

            assert next(test_instance.borrar_datos("borrar todo")) == "todo"
            assert next(test_instance.borrar_datos("borrar todo")) == "nada"
            assert next(test_instance.borrar_datos("[1]")) == "nada"
            with open(str(test_instance.backup_single), "w", newline="", encoding="utf-8") as pass_data:
                data_writer = csv.writer(pass_data, delimiter="#")
                with open(str(self.case_data_single), "r", newline="", encoding="utf-8") as has_data:
                    read = csv.reader(has_data, delimiter="#")
                    for line in read:
                        data_writer.writerow(line)

    def test_pass_object_no_dict(self):
        """chequea que se de una advertencia si se intenta guardar un objeto que no
        soporte __dict__ ya que es lo que se usa para guardar los atributos"""
        assert "Actualmente esta ocupando un objeto de tipo" in CsvClassSave(str(self.case_data_single), single=True, col_sep="#").guardar_datos_csv()

    def test_max_row_zero(self):
        """chequea que si se excede la capacidad designada para la cantidad de filas
        se retorne una advertencia al usuario al tratar de crear una nueva entrada"""
        # test class so the object has __dict__
        class RowsTest:
            filler = 0

        test_instance = CsvClassSave(str(data_test), object=RowsTest(), single=True, col_sep="#")
        test_instance.max_row_limit = 0
        assert "Advertencia: Su entrada no fue creada" in test_instance.guardar_datos_csv()
        test_instance.max_row_limit = 13
        test_instance.current_rows = 13
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
        local_instance = CsvClassSave(str(data_test), self.SingleObjectTest(**self.arguments), True, "#", exclude=("other",))
        assert local_instance.guardar_datos_csv() == "\nINDICE#START#END#START_VALS#SOLVING_TIME#DIFFICULTY#SIZE\n[21]#-432-3---1-442-3#1432234131244213#9#0.0#8#4"
        local_instance.object.end = ":" +  local_instance.object.end
        # comprobando que en modo single = True los : no son remplazados
        assert local_instance.guardar_datos_csv() == "\nINDICE#START#END#START_VALS#SOLVING_TIME#DIFFICULTY#SIZE\n[22]#-432-3---1-442-3#:1432234131244213#9#0.0#8#4"
        local_instance.exclude = ("!", "other")
        # esto lanza error (como debería) por que es modo single y la cantidad de atributos
        # no calza al negar exclude con los de las entradas ya escritas
        with pytest.raises(ValueError, match="en modo single = True"):
            local_instance.guardar_datos_csv()
        # you must iterate over it to delete all the entries
        for _ in local_instance.borrar_datos("[21:]"):
            pass

    def test_enforce_single(self):
        """comprueba que se pasen los argumentos apropiados al método guardar_datos_csv
        y que al pasar los correctos se aplique la función de enforce_unique que es 
        comprobar que solo se puedan escribir nuevas entradas con valores únicos para los
        campos pasados al argumento enforce_unique"""
        local_instance = CsvClassSave(str(data_test), self.SingleObjectTest(**self.arguments), True, "#", exclude=("other",))
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
        create_path_instance = CsvClassSave(str(data_test), single=False, col_sep="|")
        with open(str(create_path_instance.backup_multi), "w", newline="", encoding="utf-8") as pass_data:
            data_writer = csv.writer(pass_data, delimiter="|")
            with open(str(self.case_data_multiple), "r", newline="", encoding="utf-8") as has_data:
                read = csv.reader(has_data, delimiter="|")
                for line in read:
                    data_writer.writerow(line)
        multi_test_instance = CsvClassSave(str(data_test), single=False, col_sep="|")
                  # en modo multiple no hay soporte para operadores como el or y and
                  # del modo simple para buscar, aparte de eso la funcionalidad 
                  # para buscar es la misma entre los dos modos (en modo multiple
                  # para buscar por atributo (columna en modo single) usas 
                  # nombre_columna: valor)
        search = {'"marca" = Ford & velocidad = 180': [[], 0], "marca: Ford & velocidad: 180": [[], 0],
                  '"marca" = ford | velocidad >= 180': [[], 0], "marca: Ford": [[1, 11], 2], "marca: ford": [[1, 11], 2],
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
        deleted_entries = []
        for item in local_instance.borrar_datos(delete_index="<class 'vehiculo.Particular'>"):
            deleted_entries.append(str(item).split("|")[0])
        deleted_entries.pop(0)
        assert deleted_entries == ["[1]", "[9]"]
        assert local_instance.current_classes == ["<class 'vehiculo.Carga'>", "<class 'vehiculo.Bicicleta'>", 
                                                  "<class 'vehiculo.Motocicleta'>", "<class 'vehiculo.Automovil'>"]
        with open(str(local_instance.backup_multi), "w", newline="", encoding="utf-8") as pass_data:
            data_writer = csv.writer(pass_data, delimiter="|")
            with open(str(self.case_data_multiple), "r", newline="", encoding="utf-8") as has_data:
                read = csv.reader(has_data, delimiter="|")
                for line in read:
                    data_writer.writerow(line)


    def test_not_matched_dict_object_multiple(self):
        """comprobando que en modo multiple se puedan guardar objetos con distinto
        número y nombre de atributos (esta es la función que implementa el modo multiple a
        diferencia del modo single donde solo un tipo de objetos de igual numero y nombre 
        de atributos se debe guardar)
        """
        local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**self.arguments), False, "|")
        has_to_be = ("\nINDICE|CLASE|ATRIBUTOS\n[12]|"
                    "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                    "nombre: Mark Test, edad: 40, nacimiento: 1984, estado: soltero, comentario: estoy tranquilo")
        assert local_instance.guardar_datos_csv() == has_to_be
        for _ in local_instance.borrar_datos("[12]"):
            pass

    def test_exclude_multiple(self):
        """comprueba que los atributos del objeto excluidos usando el
        atributo exclude sean excluidos al escribir una nueva entrada en modo
        multiple ya que hay diferencias en el código usado para hacerlo dependiendo del modo"""
        local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**self.arguments), False, "|", exclude=("comentario",))
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
        for _ in local_instance.borrar_datos("[12:]"):
            pass
 
    def test_enforce_multiple(self):
        """comprueba el funcionamiento del argumento enforce_unique
        del método guardar_datos_csv en modo multiple"""
        local_instance = CsvClassSave(str(data_test), self.MultiObjectTest(**self.arguments), False, "|")
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
        for _ in local_instance.borrar_datos("[12:]"):
            pass
        
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
        expected_str = ("\nINDICE|CLASE|ATRIBUTOS\n[12]|"
                        "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                        "nombre: Mark ; mayz, edad: 40, nacimiento: 1984, estado: ca;sado;, "
                        "comentario: ;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido")
        expected_str_2 = ("\nINDICE|CLASE|ATRIBUTOS\n[13]|"
                        "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                        "nombre: Mark ; mayz, nacimiento: 1984, estado: ca;sado;, "
                        "comentario: ;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido")
        expected_str_3 = ("\nINDICE|CLASE|ATRIBUTOS\n[13]|"
                        "<class 'test_csv.TestMultiple.MultiObjectTest'>|"
                        "nombre: Mark ; mayz, nacimiento: 1960, estado: ca;sado;, "
                        "comentario: ;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido")
        assert new_local_instance.guardar_datos_csv() == expected_str
        new_local_instance.exclude = ("edad",)
        assert new_local_instance.guardar_datos_csv() == expected_str_2
        for _ in new_local_instance.borrar_datos("[13]"):
            pass
        # enforce unique comes before exclude
        local_argument["nacimiento"] = 1960
        new_local_instance.object = self.MultiObjectTest(**local_argument)
        assert new_local_instance.guardar_datos_csv(enforce_unique=("nacimiento",)) == expected_str_3

    def test_export_multi(self):
        """comprueba la correcta exportación de datos almacenados en modo
        single = False para la creación de csv individuales a partir de un
        conjunto de clases"""
        destination = Path(fr"{Path(__file__).parent}\data\export_destination.csv")
        new_local_instance = CsvClassSave(str(data_test), None, False, col_sep="|")
        incorrect_args = [((fr"{destination.parent}\destino.csv", "vehiculo"),"de destino debe ser una ruta valida"), 
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
                       "[1]|Mark ; mayz|40|1984|ca;sado;|;;nada de lo que esta aquí me gustaría decirlo, asi que adios; me despido\n")
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
        new_local_instance.object = self.MultiObjectTest(**{"nombre": "aq;uí", "edad": 0, "nacimiento": 17, "estado": ";ahora;", "comentario": "HOLA"})
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
        


