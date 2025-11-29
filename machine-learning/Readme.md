# Machine Learning

## Table Of Contents

# Concepts

## Deploying Machine Learning Models

### 1. Deploying Machine Learning Models with FastAPI and Docker

- **Why use FastAPI for machine learning?**

  1. **Deploying prediction models**: The data science team may have trained a model for the prediction of the sales demand in a warehouse. To make it useful, they have to provide an API interface so other parts of the stock management system can use this new prediction functionality.
  2. Suggestion engines: One of the very common uses of ML is as a system that provides suggestions based on the users’ choices. For example, if someone puts certain products in their shopping cart, more items can be suggested to that user. Such an e-commerce system requires an API call to the suggestion engine that takes input parameters.
  3. **Dynamic dashboards and reporting systems**: Sometimes, reports for data science projects need to be presented as dashboards so users can inspect the results themselves. One possible approach is to have the data model provide an API. Frontend developers can use this API to create applications that allow users to interact with the data.

- **Advantages of using FastAPI**

  1. Fast development
  2. **Fast documentation**: FastAPI provides automatic interactive API documentation using [Swagger UI](), which is an industry standard. No extra effort is required to build clear documentation with API call examples. This creates an advantage for busy data science teams who may not have the energy and expertise to write technical specifications and documentation.
  3. **Easy testing**: Testing FastAPI is made simple thanks to [Starlette](https://www.starlette.io/testclient/) and [HTTPX](https://www.python-httpx.org/)
  4. **Fast deployment**: FastAPI comes with a [CLI tool](https://fastapi.tiangolo.com/fastapi-cli/) that can bridge development and deployment smoothly. It allows you to switch between development mode and production mode easily. Once development is completed, the code can be easily deployed using a [Docker container](https://fastapi.tiangolo.com/deployment/docker/) with images that have Python prebuilt.

- **Project structure**:

  - project-name/
    - app/
      - `__init__.py`
      - main.py # FastAPI application
    - models/
      - model_name.pkl # Trained model
    - train_model.py # model training script
    - requirements.txt # python dependncies
    - Dockerfile # container configuration

- Installing FastAPI

  - You’ll install FastAPI along with `uvicorn`, which is an [ASGI server](#) that serves as FastAPI's lightning-fast gateway to handling requests.
    ```sh
        pip install fastapi uvicorn
    ```

- **Setting Up First FastAPI Project**

  - To initialize a project, create a new directory for the FastAPI application. Your first file, typically named `main.py`, will house your application's code. Here's a skeleton to get you started:

    ```py
        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/")
        def read_root():
            return {"FastAPI Test!}
    ```

  - Run the application with `uvicorn` from the terminal:

    ```sh
     uvicorn main:app --reload
    ```

  - The `--reload` flag makes the server restart after code changes, which is useful during development.

- **Basic Concepts: Routing, Requests, and Responses**

  - **FastAPI** uses a simple yet powerful routing system. By decorating functions with `@app.get("/")`, you tell **FastAPI** to execute that function whenever a `GET` request is received to the root URL `"/"`. The return value of the function is automatically converted into JSON, making it easy to build APIs.
  - **FastAPI** provides a **Request** object that encapsulates all the request information and a **Response** object for sending data back to the client. Here's how you could use them:

    ```py
        from fastapi import FastAPI, Request

        app = FastAPI()

        @app.get("/items/{item_id}")
        def read_item(item_id: int, request: Request):
            return {"item_id": item_id, "request": request.method}
    ```

  - This snippet demonstrates path parameters and request objects. The `item_id` in the path will be captured and passed to your function, and you can access various request details via the **Request** object.

- **Integrating ML Models with FastAPI**

  - FastAPI’s design caters to the seamless integration of machine learning models. Whether your model is a simple scikit-learn model or a complex deep learning model built with TensorFlow or PyTorch, integrating it with FastAPI follows similar steps:
  - Step 1: **Load Model**

    - Start by loading your trained machine learning model into your application. This could be done during the startup event of **FastAPI**, ensuring that the model is loaded into memory when the application starts.

      ```py
        from fastapi import FastAPI
        from your_ml_library import load_model

        app = FastAPI()
        model = load_model("your_model_path")

        @app.on_event("startup")
        async def load_model():
            global model
            model = load_model("your_model_path")
      ```

  - Step 2: **Create Prediction Endpoint**

    - Define an API endpoint that receives input data, processes it, and returns predictions made by your model.
      ```py
        @app.post("/predict")
        async def get_prediction(data: YourInputType):
            processed_data = preprocess(data)
            prediction = model.predict(processed_data)
            return {"prediction": prediction}
      ```

  - Step 3: **Preprocessing and Postprocessing**:
    - Implement functions to preprocess the incoming data into a format your model expects and to postprocess the model’s predictions into a human-readable or client-specific format.

- **Handling Asynchronous Operations**

  - FastAPI’s support for **asynchronous operations** allows you to handle non-blocking tasks efficiently, which is particularly useful when dealing with I/O-bound operations like reading from a database or making HTTP requests:
  - **Asynchronous Endpoints**: Make your endpoint handlers asynchronous, which allows FastAPI to manage other requests while waiting for I/O operations to complete.
    ```py
        @app.get("/items/{item_id}")
        async def read_item(item_id: int):
            item = await get_item_from_db(item_id)
            return {"item": item}
    ```
  - **Background Tasks**: For operations that can be executed after the response has been sent to the client, use FastAPI’s BackgroundTasks to run these tasks without keeping the client waiting.

    ```py
        from fastapi import BackgroundTasks

        def write_log(message: str):
            # Imagine a function that writes log messages to a file
            pass

        @app.post("/predict")
        async def get_prediction(background_tasks: BackgroundTasks, data: YourInputType):
            prediction = model.predict(data)
            background_tasks.add_task(write_log, f"Prediction made: {prediction}")
            return {"prediction": prediction}
    ```

- **Optimizing Performance for ML Models**: To ensure that your machine learning models perform optimally in a FastAPI deployment:

  1.  **Use Asynchronous Libraries**: When interacting with databases or making HTTP requests within your FastAPI app, use asynchronous libraries to prevent blocking the main execution thread.
  2.  **Concurrent Model Inference**: If your model supports it, use concurrency in model inference to handle multiple requests simultaneously.
  3.  **Model Loading**: Load your model into memory just once when the application starts, rather than loading it with each request, to save time and resources.
  4.  **Efficient Data Serialization**: Use Pydantic models for data validation and serialization. They not only enforce type safety but also offer fast serialization.

- **Advanced Features of FastAPI**

  1. **Dependency Injection**

     - Dependency Injection (DI) in FastAPI allows you to create dependencies that can be reused across multiple endpoints. This is particularly useful for database connections, authentication, and shared logic.

       ```py
        from fastapi import Depends, FastAPI

        def get_db():
            db = DBSession()
            try:
                yield db
            finally:
                db.close()

        @app.get("/items/")
        async def read_items(db = Depends(get_db)):
            items = db.get_items()
            return items
       ```

  2. **Security and Authentication**

     - FastAPI provides tools to handle authentication and security seamlessly, including OAuth2 with Password (and hashing), JWT tokens, and HTTP Basic Auth.

       ```py
        from fastapi import Depends, FastAPI, HTTPException, status
        from fastapi.security import OAuth2PasswordBearer

        oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

        @app.get("/users/me")
        async def read_users_me(token: str = Depends(oauth2_scheme)):
            user = decode_token(token)
            return user
       ```

  3. **Data Validation and Serialization with Pydantic**

     - **Pydantic** is used for data validation and settings management using Python type annotations. Pydantic ensures that the incoming request data is of the correct type and adheres to the specified schema, which is critical for machine learning models that expect data in a particular format.

       ```py
        from pydantic import BaseModel

        class Item(BaseModel):
            name: str
            description: str = None
            price: float
            tax: float = None

        @app.post("/items/")
        async def create_item(item: Item):
            return item
       ```

     - These features make FastAPI a highly extensible and secure framework for deploying machine learning models, providing robust tools to build APIs that are not only fast but also reliable and maintainable.

# Resources and Further Reading

1. [Medium - FastAPI: The Modern Toolkit for Machine Learning Deployment](https://medium.com/@reza.shokrzad/fastapi-the-modern-toolkit-for-machine-learning-deployment-af31d72b6589)
