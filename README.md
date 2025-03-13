Este proyecto implementa una API en FastAPI que gestiona datos de departamentos, trabajos y empleados. Utiliza SQLite como base de datos y permite subir archivos CSV para poblar las tablas. Se ha dockerizado la aplicación y se ha desplegado en AWS EC2.

Estructura del Proyecto
├── main.py               
├── test_main.py           
├── Dockerfile             
├── requirements.txt       
├── globant-key.pem        
└── README.md             

Instalación y Ejecución Local
Clonar el repositorio
git clone https://github.com/Soren0608/globant-data-challenge.git
cd tu-repositorio

Crear un entorno virtual (opcional, recomendado)
python -m venv venv
source venv/bin/activate 

Instalar dependencias
pip install -r requirements.txt

Iniciar la API
uvicorn main:app --reload

Ejecución con Docker

Construir la imagen Docker
docker build -t globant_api .

Ejecutar el contenedor
docker run -d -p 8000:8000 --name globant_container globant_api

Verifica que el contenedor esté corriendo
docker ps

Ejecutar la API en EC2

Acceder desde el navegador a:
http://18.218.49.226:8000/docs
