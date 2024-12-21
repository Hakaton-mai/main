from flask import Blueprint, request, jsonify
import logging
from get_from_click import get_by_category

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

reviews_controller = Blueprint('reviews', __name__)

@reviews_controller.route('/reviews/category', methods=['GET'])
def get_reviews_by_category():
    """
    получение отзывов по категории.
    """
    category = request.args.get('category')

    if not category:
        return jsonify({"error": "Параметр 'category' обязателен."}), 400

    try:
        results = get_by_category(category, 1000)
        return jsonify(results), 200
    except Exception as e:
        logger.error(f"Ошибка извлечения отзывов: {e}")
        return jsonify({"error": str(e)}), 500
