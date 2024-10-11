# Numpy

## Table Of Contents

- []()
- [Creating NumPy Arrays]()

  - [Creating NumPy Arrays from Scratch](https://colab.research.google.com/drive/1Df0ZGCj1ly_YNoqwKhqk3GwyDXXymWRT#scrollTo=yDnTUiE-NwNt)

  - [Creating NymPy Arrays from Python List](https://colab.research.google.com/drive/1bWxGkQmUOtMWY2Q4ITihdHANaghJZoOn)

  - [NumPy Random Seed](https://colab.research.google.com/drive/1I50We2QvWfrL6BWx3ZUn9Hk5FqHE_1sL#scrollTo=Tb3uTEh_SB-5)

  - [NumPy UFuncs](https://colab.research.google.com/drive/16kl5epj__zuTrQxgkLFZZOuDKwqBdMbC#scrollTo=wUjwddnmVLLY)

  - [Computations on NumPy Arrays](https://colab.research.google.com/drive/1CmXkvuxIM9RUcU92Ztc4lNz0Zxj8jALs)

# Introduction to NumPy Arrays

- **NumPy** arrays contain values of a single type, so it is important to have detailed knowledge of those types and their limitations.

# Creating NumPy Arrays

## 1. Creating NumPy Arrays From Scratch

- Examples:
  1. Create a length - 10 integer array filled with zeros
     ```py
        zeros_arrays = np.zeros(10, dtype = 'int')
        zeros_arrays
     ```
     Output:
     ```sh
        array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
     ```
  2. Create a `3 * 5` floating - point array filled with ones
     ```py
        ones_array = np.ones((3, 5), dtype = 'float')
        ones_array
     ```
  3. Create a `3 * 5` array filled with 3.14
     ```py
        full_array = np.full((3, 5), 3.14)
     ```
  4. Create an array filled with a linear sequence
     ```py
        arange_array = np.arange(0, 20, 2)
     ```
  5. Create an array of five values evenly spaced between 0 and 1
     ```py
        linspace_array = np.linspace(0, 1, 5)
     ```
  6. Create a 3x3 array of uniformly distributed
     ```py
        random_array = np.random.random((3, 3)) # random values between 0 and 1
     ```
  7. Create a 3x3 array of normally distributed random values
     ```py
        normal_array = np.random.normal(0, 1, (3, 3)) # with mean 0 and standard deviation 1
     ```
  8. Create a 4x4 array of random integers in the interval `[0, 10)`
     ```py
        random_integer_array = np.random.randint(0, 10, (4, 4))
     ```
  9. Create a 4x4 identity matrix
     ```py
        identity_array = np.eye(4)
     ```
  10. Create an uninitialized array of three integers
      ```py
        empty_array = np.empty(4)
      ```

## 2. Creating NumPy Arrays From Python Lists

- we can use np.array to create arrays from Python lists:
- Examples:
  1. Integer Arrays
     ```python
        np.array([1, 2, 3, 4])
     ```
     Output:
     ```sh
        array([1, 2, 3, 4])
     ```
  2. **Remark**: unlike Python lists, **NumPy** is constrained to arrays that all contain the same type. If types do not match, **NumPy** will upcast if possible. Here, **integers** are upcast to floating point
     ```py
        np.array([3.14, 5, 8, 6])
     ```
     Output:
     ```sh
         array([3.14, 5.  , 8.  , 6.  ])
     ```
  3. **Remark**: If we want to explicitly set the data type of the resulting array, we can use the `dtype` keyword
     ```python
        np.array([4.14, 7.8, 5, 8], dtype = 'float32')
     ```
     Output:
     ```sh
         array([4.14, 7.8 , 5.  , 8.  ], dtype=float32)
     ```
  4. **Remark**: unlike Python lists, NumPy arrays can explicitly be multidimensional. here’s one way of initializing a multidimensional array using a list of lists:
     ```py
        np.array([range(i, i + 3) for i in[2, 4, 6]])
     ```

# Resources and Further Reading
