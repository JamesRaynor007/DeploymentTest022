import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List
import os

# Definir la ruta de los archivos CSV
resultado_crew_path = os.path.join(os.path.dirname(__file__), 'resultado_crew.csv')
funcion_director_path = os.path.join(os.path.dirname(__file__), 'FuncionDirector.csv')
resultado_cast_actores_path = os.path.join(os.path.dirname(__file__), 'ResultadoCastActores.csv')
funcion_actor_path = os.path.join(os.path.dirname(__file__), 'FuncionActor.csv')
lista_actores_path = os.path.join(os.path.dirname(__file__), 'ListaActores.csv')

app = FastAPI()

# Cargar los datasets
try:
    resultado_crew = pd.read_csv(resultado_crew_path)
    funcion_director = pd.read_csv(funcion_director_path)
    ResultadoCastActores = pd.read_csv(resultado_cast_actores_path)
    funcion_actor = pd.read_csv(funcion_actor_path)
    lista_actores = pd.read_csv(lista_actores_path)
except FileNotFoundError as e:
    raise HTTPException(status_code=500, detail=f"Error al cargar los archivos: {str(e)}")

# Modelo para la información de la película
class MovieInfo(BaseModel):
    title: str
    release_date: str
    return_: str  # Retorno como porcentaje
    budget: str   # Presupuesto formateado
    revenue: str  # Ingresos formateados

class DirectorResponse(BaseModel):
    resultado_texto: str  # Agregar el texto descriptivo aquí
    movies: List[MovieInfo]

# Mensaje de bienvenida
@app.get("/", tags=["Bienvenida"])
def welcome(request: Request):
    base_url = str(request.base_url)
    
    return {
        "message": "Bienvenido a la API de Análisis de Cine.",
        "functions": {
            f"{base_url}/director/{{director_name}}": "Obtiene información sobre un director específico, incluyendo sus películas y ingresos totales.",
            f"{base_url}/directores": "Devuelve una lista de todos los directores disponibles en la base de datos.",
            f"{base_url}/actor/{{actor_name}}": "Obtiene el rendimiento financiero del actor especificado.",
            f"{base_url}/actores": "Lista todos los actores disponibles en la base de datos."
        },
        "examples": {
            "Get Director Info": f"Ejemplo: {base_url}director/Quentin%20Tarantino",
            "Get All Directors": f"Ejemplo: {base_url}directores",
            "Get Actor Info": f"Ejemplo: {base_url}actor/Leonardo%20DiCaprio",
            "Get All Actors": f"Ejemplo: {base_url}actores"
        }
    }

@app.get("/director/{director_name}", response_model=DirectorResponse)
def get_director_info(director_name: str):
    director_name_lower = director_name.lower()
    director_movies = resultado_crew[resultado_crew['name'].str.lower() == director_name_lower]

    if director_movies.empty:
        raise HTTPException(status_code=404, detail="Director no encontrado")

    director_movies = director_movies.merge(funcion_director, left_on='movie_id', right_on='id', how='inner')

    total_revenue = director_movies['revenue'].sum()
    total_return = director_movies['return'].sum()
    average_return = total_return / len(director_movies) if len(director_movies) > 0 else 0

    non_zero_returns = director_movies[director_movies['return'] > 0]
    average_return_non_zero = non_zero_returns['return'].mean() if len(non_zero_returns) > 0 else 0

    total_movies = len(director_movies)
    zero_return_movies = director_movies[director_movies['return'] == 0]
    total_zero_return = len(zero_return_movies)

    # Generar la lista de información de las películas
    movies_info = [
        MovieInfo(
            title=row['title'],
            release_date=row['release_date'],
            return_=f"{row['return']:.2f}%",
            budget=f"${row['budget']:,.2f}",
            revenue=f"${row['revenue']:,.2f}"
        ) for index, row in director_movies.iterrows()
    ]

    # Generar el texto descriptivo
    resultado_texto = (
        f"El director {director_name} ha obtenido una ganancia total de {total_revenue:,.2f}, "
        f"con un retorno total promedio de {average_return:.2f}% en un total de {total_movies} películas, "
        f"y con un retorno de {average_return_non_zero:.2f}% sin contar las {total_zero_return} "
        f"películas que no tienen retorno en este dataset."
    )

    return DirectorResponse(
        resultado_texto=resultado_texto,  # Incluir el texto descriptivo aquí
        movies=movies_info
    )

@app.get("/directores")
def obtener_directores():
    directores = resultado_crew['name'].unique().tolist()
    return directores

@app.get("/actor/{actor_name}")
def obtener_retorno_actor(actor_name: str):
    if not actor_name:
        raise HTTPException(status_code=400, detail="El nombre del actor no puede estar vacío.")
    
    actor_name_normalizado = actor_name.lower()
    peliculas_actor = ResultadoCastActores[ResultadoCastActores['name'].str.lower() == actor_name_normalizado]

    if peliculas_actor.empty:
        raise HTTPException(status_code=404, detail="Actor no encontrado")

    movie_ids = peliculas_actor['movie_id'].tolist()
    ganancias_actor = funcion_actor[funcion_actor['id'].isin(movie_ids)]

    retorno_total = ganancias_actor['return'].sum()
    ganancias_validas = ganancias_actor[ganancias_actor['return'] > 0]
    cantidad_peliculas_validas = len(ganancias_validas)

    if cantidad_peliculas_validas > 0:
        retorno_promedio = round(ganancias_validas['return'].mean(), 2) * 100
    else:
        retorno_promedio = 0.0

    retorno_total_formateado = f"{retorno_total * 100:,.2f}%"
    retorno_promedio_formateado = f"{retorno_promedio:,.2f}%"

    peliculas_con_return_zero = ganancias_actor[ganancias_actor['return'] == 0]['id'].tolist()
    peliculas_con_return_zero_count = len(peliculas_con_return_zero)

    # Generar el texto descriptivo
    resultado_texto = (
        f"El actor {actor_name} ha actuado en {len(ganancias_actor)} películas, "
        f"con un retorno total de {retorno_total_formateado}, "
        f"y un retorno promedio de {retorno_promedio_formateado}. "
        f"La cantidad de películas sin retorno en el dataset son {peliculas_con_return_zero_count}, "
        f"el retorno promedio contándolas es de {round(retorno_total / len(ganancias_actor) * 100, 2):,.2f}%."
    )

    return {"resultado": resultado_texto}

@app.get("/actores")
def listar_actores():
    actores_lista = lista_actores['name'].str.lower().tolist()
    return {"actores": actores_lista}
