from flask import Flask
from .routes import setup_routes

def create_app():
    app = Flask(__name__)
    
    # Setup configuration
    app.config.from_object('app.config.Config')
    
    # Register routes
    setup_routes(app)
    
    return app
