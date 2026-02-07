from tern.db_uitls import ensure_creator, User


def ensure_creator_user():
    user = User(username="test_creator", password="test_creator")
    super_user = User(username="postgres", password="postgres")
    ensure_creator(user, super_user)


if __name__ == "__main__":
    ensure_creator_user()
