# Analytics With Python

## Table Of Contents

- [Setting Up Python Virtual Environment](#setting-up-virtual-environment)
- [Python Modules For Data Analysis](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Python_Modules_for_Data_Analysis)
  - [Pandas]()
  - [Numpy]()
  - [Matplotlib]()
  - [Scikit Learn]()
- [Web Scrapping](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Web_Scrapping)
- [Retention Analysis](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Retention_Analysis)

# Configure VS Code for Python Environment

## Python Extension

- Install [Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) from Microsoft.
- This is actually an extension pack that contains two extensions.
  - The first extension is the Python extension. It lays the foundation for Python development in Visual Studio Code.
  - The other one is [Pylance](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance), which is a very performant language server for Python.

## Ruff Linter

- Ruff is an extremely fast Python linter written in Rust that imposes stricter linting rules than Pylint.
- Install [Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)

## Isort

- Like a linter, [isort](https://marketplace.visualstudio.com/items?itemName=ms-python.isort) is another utility that's sole purpose is sorting import statements.
- The utility sorts all the imports alphabetically, while also dividing them into sections.

## InteiCode

- [InteiCode](https://marketplace.visualstudio.com/items?itemName=VisualStudioExptTeam.vscodeintellicode) provides AI assisted code completion in Visual Studio Code.

## Error Lens

- While not related to Python specifically, [Error Lens](https://marketplace.visualstudio.com/items?itemName=usernamehw.errorlens) is a great extension that embeds errors right by the side of the line of code.

## Indent Rainbow

- [indent-rainbow](https://marketplace.visualstudio.com/items?itemName=oderwat.indent-rainbow) adds some color to the identation.

# Setting Up Virtual Environment

1. Navigate to the root directory of your project using the terminal or command prompt.

1. Run the following command to create a new virtual environment: This will create a new directory called **env** in your project directory, which contains the virtual environment.

   ```sh
       python -m venv venv
   ```

1. Activate the virtual environment by running the following command: This will activate the virtual environment and change your shell's prompt to indicate that you're working within the virtual environment.

   ```sh
       source env/bin/activate  # For Linux or macOS
       source venv/Scripts/activate  # For Windows
   ```

1. You can now install any required modules using pip, which will install them within the virtual environment. For example:

   ```sh
       pip install numpy
   ```

   This will install the numpy module within the virtual environment, making it available for use in your project.

# Remarks

- _When you're done working on your project, you can deactivate the virtual environment by running the deactivate command in the terminal._

- _By creating a virtual environment within your project directory, you can easily share your project with others, since they can create their own virtual environment and install the required dependencies using the **requirements.txt** file (which lists the required packages and versions) that you can provide along with your code._

- _it's a best practice to use a separate file, usually called **requirements.txt** or **Pipfile**, to list the required dependencies for your project. You can generate this file automatically using **pip freeze**, which will list all of the installed packages and their versions within your virtual environment. Here's an example of how to generate a **requirements.txt** file:_

  ```sh
      pip freeze > requirements.txt
  ```

  This will create a requirements.txt file in your project directory that lists all of the installed packages and their versions. Commit the **requirements.txt** file to your git repository.
