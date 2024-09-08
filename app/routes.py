from flask import request, jsonify
from .tasks import DataPipeline, task_registry
from .utils import load_dataset
from .tasks import DataPipelineTask
from .tasks import add_task_to_library, update_task_in_library


def setup_routes(app):
    @app.route('/run-pipeline', methods=['POST'])
    def run_pipeline():
        file_path = 'dataset.csv'
        df = load_dataset(file_path)
        
        if isinstance(df, str):
            return jsonify({"error": f"Unable to load dataset: {df}"}), 400

        task_list = request.json.get('tasks', [])
        
        pipeline = DataPipeline(task_registry)
        results = pipeline.run(df, task_list)
        
        return jsonify(results)

    @app.route('/add-task', methods=['POST'])
    def add_task():
        data = request.json
        task_name = data.get('task_name')
        task_func_code = data.get('task_func_code')
        
        if not task_name or not task_func_code:
            return jsonify({"error": "Missing task_name or task_func_code"}), 400
        
        # Dynamically define the function from the provided code
        try:
            # Define a new task function dynamically
            exec(task_func_code, globals())
            
            # Ensure the function is available in globals
            if task_name in globals():
                task_func = globals()[task_name]
                # Add the task to the registry
                result = add_task_to_library(task_name, task_func)
                return jsonify({"message": result}), 200
            else:
                return jsonify({"error": f"Task function '{task_name}' not defined"}), 400
        except Exception as e:
            return jsonify({"error": f"Error adding task: {e}"}), 400

    @app.route('/update-task', methods=['POST'])
    def update_task():
        data = request.json
        task_name = data.get('task_name')
        task_func_code = data.get('task_func_code')
        
        if not task_name or not task_func_code:
            return jsonify({"error": "Missing task_name or task_func_code"}), 400
        
        try:
            # Define a new task function dynamically
            exec(task_func_code, globals())
            
            if task_name in globals():
                task_func = globals()[task_name]
                result = update_task_in_library(task_name, task_func)
                return jsonify({"message": result}), 200
            else:
                return jsonify({"error": f"Task function '{task_name}' not defined"}), 400
        except Exception as e:
            return jsonify({"error": f"Error updating task: {e}"}), 400