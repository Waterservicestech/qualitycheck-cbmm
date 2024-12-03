from tkinter import Tk, filedialog
from pathlib import Path
import eel

@eel.expose
def getFileEDD(path=''):
    """
    Opens a file dialog for the user to select an Excel file.

    Args:
        path (str, optional): The initial directory or title for the dialog. Default is ''.

    Returns:
        str: The full file path selected by the user, or an empty string if no file is selected.
    """
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    file = filedialog.askopenfilename(
        title=path,
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    file = file if type(file) != tuple else ''
    file = str(Path(file))
    file = file if file != '.' else ''
    return file

@eel.expose
def getFolder(path=''):
    """
    Opens a folder dialog for the user to select a directory.

    Args:
        path (str, optional): The initial directory or title for the dialog. Default is ''.

    Returns:
        str: The full folder path selected by the user, or an empty string if no folder is selected.
    """
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    folder = filedialog.askdirectory(title=path)
    folder = folder if type(folder) != tuple else ''
    folder = str(Path(folder))
    folder = folder if folder != '.' else ''
    return folder
