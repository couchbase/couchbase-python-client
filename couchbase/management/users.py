from typing import (TYPE_CHECKING,
                    Any,
                    Iterable)

from couchbase.management.logic.users_logic import Origin  # noqa: F401
from couchbase.management.logic.users_logic import Role  # noqa: F401
from couchbase.management.logic.users_logic import RoleAndOrigins  # noqa: F401
from couchbase.management.logic.users_logic import (Group,
                                                    RoleAndDescription,
                                                    User,
                                                    UserAndMetadata,
                                                    UserManagerLogic)
from couchbase.management.logic.wrappers import UserMgmtWrapper

if TYPE_CHECKING:
    from couchbase.management.options import (DropGroupOptions,
                                              DropUserOptions,
                                              GetAllGroupsOptions,
                                              GetAllUsersOptions,
                                              GetGroupOptions,
                                              GetRolesOptions,
                                              GetUserOptions,
                                              UpsertGroupOptions,
                                              UpsertUserOptions)


class UserManager(UserManagerLogic):

    def __init__(self, connection):
        super().__init__(connection)

    @UserMgmtWrapper.block(UserAndMetadata, UserManagerLogic._ERROR_MAPPING)
    def get_user(self,
                 username,  # type: str
                 *options,  # type: GetUserOptions
                 **kwargs   # type: Any
                 ) -> UserAndMetadata:
        return super().get_user(username, *options, **kwargs)

    @UserMgmtWrapper.block(UserAndMetadata, UserManagerLogic._ERROR_MAPPING)
    def get_all_users(self,
                      *options,  # type: GetAllUsersOptions
                      **kwargs  # type: Any
                      ) -> Iterable[UserAndMetadata]:
        return super().get_all_users(*options, **kwargs)

    @UserMgmtWrapper.block(None, UserManagerLogic._ERROR_MAPPING)
    def upsert_user(self,
                    user,     # type: User
                    *options,  # type: UpsertUserOptions
                    **kwargs  # type: Any
                    ) -> None:

        super().upsert_user(user, *options, **kwargs)

    @UserMgmtWrapper.block(None, UserManagerLogic._ERROR_MAPPING)
    def drop_user(self,
                  username,  # type: str
                  *options,  # type: DropUserOptions
                  **kwargs   # type: Any
                  ) -> None:
        super().drop_user(username, *options, **kwargs)

    @UserMgmtWrapper.block(RoleAndDescription, UserManagerLogic._ERROR_MAPPING)
    def get_roles(self,
                  *options,  # type: GetRolesOptions
                  **kwargs   # type: Any
                  ) -> Iterable[RoleAndDescription]:
        return super().get_roles(*options, **kwargs)

    @UserMgmtWrapper.block(Group, UserManagerLogic._ERROR_MAPPING)
    def get_group(self,
                  group_name,   # type: str
                  *options,     # type: GetGroupOptions
                  **kwargs      # type: Any
                  ) -> Group:
        return super().get_group(group_name, *options, **kwargs)

    @UserMgmtWrapper.block(Group, UserManagerLogic._ERROR_MAPPING)
    def get_all_groups(self,
                       *options,    # type: GetAllGroupsOptions
                       **kwargs     # type: Any
                       ) -> Iterable[Group]:
        return super().get_all_groups(*options, **kwargs)

    @UserMgmtWrapper.block(None, UserManagerLogic._ERROR_MAPPING)
    def upsert_group(self,
                     group,     # type: Group
                     *options,  # type: UpsertGroupOptions
                     **kwargs   # type: Any
                     ) -> None:
        super().upsert_group(group, *options, **kwargs)

    @UserMgmtWrapper.block(None, UserManagerLogic._ERROR_MAPPING)
    def drop_group(self,
                   group_name,  # type: str
                   *options,    # type: DropGroupOptions
                   **kwargs     # type: Any
                   ) -> None:
        super().drop_group(group_name, *options, **kwargs)
