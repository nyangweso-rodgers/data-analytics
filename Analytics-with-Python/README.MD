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

# Setting Up Virtual Environment
1. Navigate to the root directory of your project using the terminal or command prompt.

1. Run the following command to create a new virtual environment: This will create a new directory called __env__ in your project directory, which contains the virtual environment.

    ```sh
        python -m venv venv
    ```
2. Activate the virtual environment by running the following command: This will activate the virtual environment and change your shell's prompt to indicate that you're working within the virtual environment.

    ```sh
        source env/bin/activate  # For Linux or macOS
        source venv/Scripts/activate  # For Windows
    ```

3. You can now install any required modules using pip, which will install them within the virtual environment. For example:

    ```sh
        pip install numpy
    ```

    This will install the numpy module within the virtual environment, making it available for use in your project.

# Remarks
* _When you're done working on your project, you can deactivate the virtual environment by running the deactivate command in the terminal._

* _By creating a virtual environment within your project directory, you can easily share your project with others, since they can create their own virtual environment and install the required dependencies using the __requirements.txt__ file (which lists the required packages and versions) that you can provide along with your code._

* _it's a best practice to use a separate file, usually called __requirements.txt__ or __Pipfile__, to list the required dependencies for your project. You can generate this file automatically using __pip freeze__, which will list all of the installed packages and their versions within your virtual environment. Here's an example of how to generate a __requirements.txt__ file:_

    ```sh
        pip freeze > requirements.txt
    ```

    This will create a requirements.txt file in your project directory that lists all of the installed packages and their versions. Commit the __requirements.txt__ file to your git repository.